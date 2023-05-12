defmodule Xandra.Clusters.Cluster do
  @enforce_keys [
    :cluster_name,
    :address,
    :port,
    :load_balancing,
    :keyspace,
    :transport,
    :protocol_version,
    :options,
    :backoff,
    :attempts,
    :peers,
    :buffer
  ]

  defstruct [
    :cluster_name,
    :address,
    :port,
    :load_balancing,
    :keyspace,
    :transport,
    :protocol_version,
    :options,
    :backoff,
    :attempts,
    :peers,
    :buffer,
    :cql_version,
    :partitioner,
    :data_center,
    :token_ring,
    :protocol_module,
    :socket,
    :timer,
    :error
  ]

  use Connection

  alias __MODULE__
  alias Xandra.{Frame, Simple, TableMetadata, StatusChange, TopologyChange, Connection.Utils}
  alias Xandra.Clusters.Application, as: Clusters
  alias Xandra.Clusters.{ControlRegistry, ConnectionRegistry, Controls, Control}

  alias DBConnection.Backoff

  require Logger

  @max_attempts 5
  @backoff Backoff.new(backoff_type: :rand_exp, backoff_min: 1_000, backoff_max: 30_000)
  @connection_timeout 5_000
  @discover_interval 60_000
  # this has to be larger than the `discover_interval`
  @startup_delay 120_000
  @transport_options [packet: :raw, mode: :binary, keepalive: true, nodelay: true]

  @exposed_state [
    :cluster_name,
    :address,
    :port,
    :load_balancing,
    :keyspace,
    :protocol_version,
    :protocol_module,
    :options,
    :cql_version,
    :partitioner,
    :data_center,
    :token_ring
  ]

  @system_peers_query %Simple{
    statement: "SELECT * FROM system.peers",
    values: [],
    default_consistency: :one
  }

  def report_failure(cluster, {cluster_name, host_id, rpc_address, port}) do
    Logger.warn(
      "Received report on failed node for cluster [#{cluster_name}], at [#{rpc_address}:#{port}]@[#{host_id}]"
    )

    GenServer.cast(cluster, {:report_failure, {cluster_name, host_id, rpc_address, port}})
  end

  def info(cluster) do
    with nil <- Clusters.lookup_cluster_info(cluster) do
      Connection.call(cluster, :info)
    end
  end

  def register_cluster_info(cluster, state) do
    info = Map.take(state, @exposed_state)
    Clusters.register_cluster_info(cluster, info)
  end

  @impl true
  def init(options) do
    cluster_name = Keyword.fetch!(options, :cluster_name)
    address = Keyword.fetch!(options, :address)
    port = Keyword.fetch!(options, :port)

    load_balancing = Keyword.get(options, :load_balancing)
    keyspace = Keyword.get(options, :keyspace)
    encryption = Keyword.get(options, :encryption)
    transport = if encryption, do: :ssl, else: :gen_tcp
    protocol_version = Keyword.get(options, :protocol_version)

    Process.flag(:trap_exit, true)

    Process.send_after(self(), :syncup_token_ring, 3000)

    state = %Cluster{
      cluster_name: cluster_name,
      address: address,
      port: port,
      load_balancing: load_balancing,
      keyspace: keyspace,
      transport: transport,
      protocol_version: protocol_version,
      options: options,
      backoff: @backoff,
      attempts: 0,
      peers: [],
      buffer: <<>>
    }

    register_cluster_info(self(), state)

    {:connect, :init, state}
  end

  @impl true
  def connect(
        _,
        %{
          cluster_name: cluster_name,
          address: address,
          port: port,
          attempts: attempts,
          error: err
        } = state
      )
      when attempts >= @max_attempts do
    Logger.error(
      "Failed to start cluster [#{cluster_name}] at [#{address}:#{port}], #{inspect(err)}"
    )

    {:stop, {:error, err}, state}
  end

  @impl true
  def connect(
        _,
        %{
          cluster_name: cluster_name,
          address: address,
          port: port,
          transport: transport,
          protocol_version: protocol_version,
          options: options,
          backoff: backoff,
          attempts: attempts,
          timer: timer
        } = state
      ) do
    Logger.debug("Starting cluster [#{cluster_name}] at [#{address}:#{port}]")

    if timer do
      Process.cancel_timer(timer)
    end

    transport_options = Keyword.put(@transport_options, :active, false)

    with {:ok, socket} <-
           transport.connect(to_charlist(address), port, transport_options, @connection_timeout),
         {:ok, {protocol_module, cql_version}} <-
           negotiate_protocol(transport, socket, protocol_version, state),
         :ok <-
           handshake_connection(transport, socket, protocol_module, cql_version, options, state),
         {:ok,
          %{
            host_id: host_id,
            rpc_address: rpc_address,
            partitioner: partitioner,
            data_center: data_center
          }} <- discover_system_local(transport, socket, protocol_module, state),
         {:ok, peers} <- discover_system_peers(transport, socket, protocol_module, state),
         :ok <- discover_system_schema(transport, socket, protocol_module, state),
         :ok <- register_to_events(transport, socket, protocol_module, state),
         :ok <- request_system_peers(transport, socket, protocol_module, state) do
      started =
        if peers == [] do
          [
            startup_control(
              cluster_name,
              host_id,
              address,
              rpc_address,
              port,
              data_center,
              options
            )
          ]
        else
          Enum.map(peers, fn %{host_id: host_id, rpc_address: rpc_address} ->
            startup_control(
              cluster_name,
              host_id,
              rpc_address,
              rpc_address,
              port,
              data_center,
              options
            )
          end)
        end

      case Enum.find(started, &match?(:ok, &1)) do
        :ok ->
          Logger.debug(
            "Successfully connected to cluster [#{cluster_name}] at [#{address}:#{port}]"
          )

          state = %{
            state
            | socket: socket,
              cql_version: cql_version,
              partitioner: partitioner,
              data_center: data_center,
              protocol_module: protocol_module,
              backoff: @backoff,
              attempts: 0,
              buffer: <<>>,
              timer: nil,
              error: nil
          }

          register_cluster_info(self(), state)

          {:ok, state}

        nil ->
          err = Enum.find(started, &match?({:error, _}, &1))

          Logger.error(
            "Error connecting to cluster [#{cluster_name}] at [#{address}:#{port}], #{inspect(err)}"
          )

          {wait, backoff} = Backoff.backoff(backoff)

          {:backoff, wait,
           %{
             state
             | backoff: backoff,
               attempts: attempts + 1,
               buffer: <<>>,
               timer: nil,
               error: err
           }}
      end
    else
      {:error, {:use_this_protocol_instead, _failed_protocol_version, protocol_version}} ->
        Logger.debug(
          "Switch to protocol #{protocol_version} for cluster [#{cluster_name}] at [#{address}:#{port}]"
        )

        state = %{
          state
          | socket: nil,
            protocol_version: protocol_version,
            buffer: <<>>,
            timer: nil
        }

        register_cluster_info(self(), state)

        {:backoff, 0, state}

      {:error, err} ->
        Logger.error(
          "Error connecting to cluster [#{cluster_name}] at [#{address}:#{port}], #{inspect(err)}"
        )

        {wait, backoff} = Backoff.backoff(backoff)

        {:backoff, wait,
         %{state | backoff: backoff, attempts: attempts + 1, buffer: <<>>, timer: nil, error: err}}
    end
  end

  @impl true
  def handle_cast({:report_failure, {cluster_name, _host_id, rpc_address, port}}, state) do
    terminate_control(cluster_name, rpc_address, port)

    {:noreply, state}
  end

  @impl true
  def handle_call(:info, _from, state) do
    {:reply, Map.take(state, @exposed_state), state}
  end

  @impl true
  def handle_info({kind, _}, state) when kind in [:tcp_passive, :ssl_passive],
    do: {:noreply, state}

  def handle_info({kind, _}, state) when kind in [:tcp_closed, :ssl_closed],
    do: {:disconnect, {:error, :closed}, state}

  def handle_info({kind, _, err}, state) when kind in [:tcp_error, :ssl_error],
    do: {:disconnect, {:error, err}, state}

  def handle_info(
        {kind, _socket, message},
        %{cluster_name: cluster_name, address: address, port: port, buffer: buffer} = state
      )
      when kind in [:tcp, :ssl] do
    Logger.debug("Receiving message from cluster [#{cluster_name}] at [#{address}:#{port}]")

    handle_message(%{state | buffer: buffer <> message})
  end

  def handle_info(:syncup_token_ring, state) do
    state = syncup_token_ring(state)
    register_cluster_info(self(), state)
    Process.send_after(self(), :syncup_token_ring, @discover_interval)

    {:noreply, state}
  end

  def handle_info(
        :request_system_peers,
        %{
          cluster_name: cluster_name,
          address: address,
          port: port,
          transport: transport,
          socket: socket,
          protocol_module: protocol_module
        } = state
      ) do
    request_system_peers(transport, socket, protocol_module, state)
    |> case do
      :ok ->
        {:noreply, state}

      err ->
        Logger.error(
          "Error requesting system.peers from cluster [#{cluster_name}] at [#{address}:#{port}], #{inspect(err)}"
        )

        {:disconnect, err, state}
    end
  end

  def handle_info(
        {:startup_control, rpc_address, port},
        %{cluster_name: cluster_name, peers: peers, data_center: data_center, options: options} =
          state
      ) do
    peer = Enum.find(peers, &match?(%{rpc_address: ^rpc_address}, &1))

    if peer do
      startup_control(
        cluster_name,
        peer[:host_id],
        rpc_address,
        rpc_address,
        port,
        data_center,
        options
      )
    else
      Logger.warn("Ignoring node [#{rpc_address}:#{port}], not discovered in peers")
    end

    {:noreply, state}
  end

  @impl true
  def disconnect(
        err,
        %{
          transport: transport,
          socket: socket,
          cluster_name: cluster_name,
          address: address,
          port: port
        } = state
      ) do
    Logger.warn(
      "Disconnecting from cluster [#{cluster_name}] at [#{address}:#{port}], #{inspect(err)}"
    )

    if socket do
      transport.close(socket)
    end

    {:connect, :init,
     %{state | socket: nil, backoff: @backoff, attempts: 0, buffer: <<>>, error: nil}}
  end

  @impl true
  def terminate(
        reason,
        %{
          cluster_name: cluster_name,
          address: address,
          port: port
        } = state
      )
      when reason in [:normal, :shutdown] do
    Logger.debug("Stopping connection with cluster [#{cluster_name}] at [#{address}:#{port}]")

    terminate_controls(cluster_name)

    state
  end

  @impl true
  def terminate({:shutdown, _}, state) do
    terminate(:shutdown, state)
  end

  @impl true
  def terminate(
        reason,
        %{
          cluster_name: cluster_name,
          address: address,
          port: port
        } = state
      ) do
    Logger.error(
      "Exiting connection with cluster [#{cluster_name}] at [#{address}:#{port}], #{inspect(reason)}"
    )

    terminate_controls(cluster_name)

    state
  end

  defp negotiate_protocol(
         transport,
         socket,
         protocol_version,
         %{cluster_name: cluster_name, address: address, port: port}
       ) do
    Logger.debug("Negotiating protocol with cluster [#{cluster_name}] at [#{address}:#{port}]")

    with {:ok, %{"CQL_VERSION" => [cql_version | _]} = _supported_options, protocol_module} <-
           Utils.request_options(transport, socket, protocol_version) do
      Logger.debug(
        "Using protocol #{protocol_module}, cql #{cql_version} with cluster [#{cluster_name}] at [#{address}:#{port}]"
      )

      {:ok, {protocol_module, cql_version}}
    end
  end

  defp handshake_connection(
         transport,
         socket,
         protocol_module,
         cql_version,
         options,
         %{cluster_name: cluster_name, address: address, port: port}
       ) do
    Logger.debug("Handshaking connection with cluster [#{cluster_name}] at [#{address}:#{port}]")

    requested_options = %{"CQL_VERSION" => cql_version}
    Utils.startup_connection(transport, socket, requested_options, protocol_module, nil, options)
  end

  defp syncup_token_ring(
         %{
           cluster_name: cluster_name,
           keyspace: keyspace,
           data_center: data_center,
           address: address,
           port: port
         } = state
       ) do
    Logger.debug(
      "Syncing up system.token_ring for keyspace [#{keyspace}] with cluster [#{cluster_name}] at [#{address}:#{port}]"
    )

    with [{conn_pid} | _] <- random_connections(cluster_name) do
      query =
        "SELECT start_token, end_token, endpoint, dc FROM system.token_ring where keyspace_name = ? LIMIT 1000000"

      stream = Xandra.stream_pages!(conn_pid, query, [{"text", keyspace}], [])

      %{rows: rows} =
        Enum.reduce_while(stream, %{rows: [], num_rows: 0}, fn
          %Xandra.Page{} = page, %{rows: rows, num_rows: num_rows} ->
            %{rows: new_rows, num_rows: new_num_rows} = process_page(page)
            {:cont, %{rows: rows ++ new_rows, num_rows: num_rows + new_num_rows}}
        end)

      token_ring =
        rows
        |> Enum.filter(fn [_, _, _, dc] -> dc == data_center end)
        |> Enum.map(fn [start_token, end_token, rpc_address, _] ->
          {start_token, ""} = Integer.parse(start_token)
          {end_token, ""} = Integer.parse(end_token)
          {start_token, end_token, rpc_address}
        end)
        |> Enum.group_by(
          fn {start_token, end_token, _} -> {start_token, end_token} end,
          fn {_, _, rpc_address} -> rpc_address end
        )
        |> Enum.into([])
        |> Enum.sort_by(fn {{start_token, _}, _} -> start_token end)

      %{state | token_ring: token_ring}
    else
      [] -> state
    end
  rescue
    err ->
      Logger.error(
        "Error syncing up system.token_ring for keyspace [#{keyspace}] with cluster [#{cluster_name}] at [#{address}:#{port}], #{inspect(err)}"
      )

      state
  end

  defp random_connections(cluster_name) do
    Registry.select(ConnectionRegistry, [{{{cluster_name, :_}, :"$1", :_}, [], [{{:"$1"}}]}])
    |> Enum.shuffle()
  end

  defp discover_system_local(
         transport,
         socket,
         protocol_module,
         %{cluster_name: cluster_name, address: address, port: port}
       ) do
    Logger.debug(
      "Discovering system.local with cluster [#{cluster_name}] at [#{address}:#{port}]"
    )

    query = %Simple{
      statement: "SELECT * FROM system.local",
      values: [],
      default_consistency: :one
    }

    payload =
      Frame.new(:query, _options = [])
      |> protocol_module.encode_request(query)
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <-
           Utils.recv_frame(transport, socket, protocol_format, _compressor = nil) do
      {%Xandra.Page{} = page, _warnings} =
        protocol_module.decode_response(%{frame | atom_keys?: true}, query)

      [system_local] = Enum.to_list(page)
      {:ok, system_local}
    end
  end

  defp discover_system_schema(_transport, _socket, _protocol_module, %{keyspace: nil}), do: :ok

  defp discover_system_schema(
         transport,
         socket,
         protocol_module,
         %{
           cluster_name: cluster_name,
           address: address,
           port: port,
           load_balancing: load_balancing,
           keyspace: keyspace
         } = state
       ) do
    if Enum.member?(load_balancing, :token_aware) do
      Logger.debug(
        "Discovering keyspace schema with cluster [#{cluster_name}] at [#{address}:#{port}]"
      )

      with {:ok, tables} <-
             discover_keyspace_tables(transport, socket, protocol_module, keyspace, state) do
        Enum.each(
          tables,
          &discover_keyspace_table(transport, socket, protocol_module, keyspace, &1, state)
        )
      end
    else
      :ok
    end
  end

  defp discover_keyspace_tables(transport, socket, protocol_module, keyspace, %{
         cluster_name: cluster_name,
         address: address,
         port: port
       }) do
    Logger.debug(
      "Discovering tables for keyspace [#{keyspace}] with cluster [#{cluster_name}] at [#{address}:#{port}]"
    )

    query = %Simple{
      statement: "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
      values: [{"text", keyspace}],
      default_consistency: :one
    }

    payload =
      Frame.new(:query, _options = [])
      |> protocol_module.encode_request(query)
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <-
           Utils.recv_frame(transport, socket, protocol_format, _compressor = nil),
         {%Xandra.Page{} = page, _warnings} <-
           protocol_module.decode_response(%{frame | atom_keys?: true}, query) do
      tables =
        page
        |> Enum.to_list()
        |> Enum.map(& &1[:table_name])

      {:ok, tables}
    end
  end

  defp discover_keyspace_table(transport, socket, protocol_module, keyspace, table, %{
         cluster_name: cluster_name,
         address: address,
         port: port
       }) do
    Logger.debug(
      "Discovering table columns for table [#{keyspace}.#{table}] with cluster [#{cluster_name}] at [#{address}:#{port}]"
    )

    query = %Simple{
      statement: "SELECT * FROM system_schema.columns WHERE keyspace_name = ? and table_name = ?",
      values: [{"text", keyspace}, {"text", table}],
      default_consistency: :one
    }

    payload =
      Frame.new(:query, _options = [])
      |> protocol_module.encode_request(query)
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <-
           Utils.recv_frame(transport, socket, protocol_format, _compressor = nil),
         {%Xandra.Page{} = page, _warnings} <-
           protocol_module.decode_response(%{frame | atom_keys?: true}, query) do
      TableMetadata.from_columns(cluster_name, Enum.to_list(page))
      |> Clusters.register_schema()

      :ok
    end
  end

  defp discover_system_peers(
         transport,
         socket,
         protocol_module,
         %{cluster_name: cluster_name, address: address, port: port}
       ) do
    Logger.debug(
      "Discovering system.peers with cluster [#{cluster_name}] at [#{address}:#{port}]"
    )

    payload =
      Frame.new(:query, _options = [])
      |> protocol_module.encode_request(@system_peers_query)
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <-
           Utils.recv_frame(transport, socket, protocol_format, _compressor = nil),
         {%Xandra.Page{} = page, _warnings} <-
           protocol_module.decode_response(%{frame | atom_keys?: true}, @system_peers_query) do
      {:ok, Enum.to_list(page)}
    end
  end

  defp request_system_peers(
         transport,
         socket,
         protocol_module,
         %{cluster_name: cluster_name, address: address, port: port}
       ) do
    Logger.debug("Requesting system.peers with cluster [#{cluster_name}] at [#{address}:#{port}]")

    payload =
      Frame.new(:query, _options = [])
      |> protocol_module.encode_request(@system_peers_query)
      |> Frame.encode(protocol_module)

    transport.send(socket, payload)
  end

  defp register_to_events(transport, socket, protocol_module, %{
         cluster_name: cluster_name,
         address: address,
         port: port
       }) do
    Logger.debug("Registering events with cluster [#{cluster_name}] at [#{address}:#{port}]")

    payload =
      Frame.new(:register, _options = [])
      |> protocol_module.encode_request(["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <-
           Utils.recv_frame(transport, socket, protocol_format, _compressor = nil),
         :ok <- protocol_module.decode_response(frame) do
      inet_mod(transport).setopts(socket, active: true)
    end
  end

  defp decode_frame(buffer) do
    header_length = Frame.header_length()

    case buffer do
      <<header::size(header_length)-bytes, rest::binary>> ->
        body_length = Frame.body_length(header)

        case rest do
          <<body::size(body_length)-bytes, rest::binary>> -> {Frame.decode(header, body), rest}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp handle_message(%{buffer: <<>>} = state) do
    {:noreply, state}
  end

  defp handle_message(%{protocol_module: protocol_module, buffer: buffer} = state) do
    case decode_frame(buffer) do
      {%Frame{kind: :result} = frame, rest} ->
        {%Xandra.Page{} = page, _warnings} =
          protocol_module.decode_response(%{frame | atom_keys?: true}, @system_peers_query)

        timer = Process.send_after(self(), :request_system_peers, @discover_interval)
        handle_message(%{state | buffer: rest, peers: Enum.to_list(page), timer: timer})

      {frame, rest} ->
        event = protocol_module.decode_response(%{frame | atom_keys?: true})
        state = handle_event(event, state)
        handle_message(%{state | buffer: rest})

      :error ->
        {:noreply, state}
    end
  end

  defp handle_event(
         %StatusChange{effect: "UP", address: up, port: port} = event,
         %{
           cluster_name: cluster_name,
           address: address,
           port: port
         } = state
       ) do
    Logger.debug(
      "Received status_change [UP] event from cluster [#{cluster_name}] at [#{address}:#{port}], #{inspect(event)}"
    )

    Process.send_after(self(), {:startup_control, up, port}, @startup_delay)
    state
  end

  defp handle_event(
         %StatusChange{effect: "DOWN", address: down} = event,
         %{
           cluster_name: cluster_name,
           address: address,
           port: port
         } = state
       ) do
    Logger.warn(
      "Received status_change [DOWN] event from cluster [#{cluster_name}] at [#{address}:#{port}], #{inspect(event)}"
    )

    terminate_control(cluster_name, down, port)

    state
  end

  defp handle_event(
         %TopologyChange{effect: "NEW_NODE", address: new, port: port} = event,
         %{
           cluster_name: cluster_name,
           address: address,
           port: port
         } = state
       ) do
    Logger.debug(
      "Received topology_change [NEW_NODE] event from cluster [#{cluster_name}] at [#{address}:#{port}], #{inspect(event)}, ignored"
    )

    Process.send_after(self(), {:startup_control, new, port}, @startup_delay)

    state
  end

  defp handle_event(
         %TopologyChange{effect: "MOVED_NODE"} = event,
         %{
           cluster_name: cluster_name,
           address: address,
           port: port
         } = state
       ) do
    Logger.warn(
      "Received topology_change [MOVED_NODE] event from cluster [#{cluster_name}] at [#{address}:#{port}], #{inspect(event)}, ignored"
    )

    state
  end

  defp handle_event(
         %TopologyChange{effect: "REMOVED_NODE", address: removed} = event,
         %{
           cluster_name: cluster_name,
           address: address,
           port: port
         } = state
       ) do
    Logger.warn(
      "Received topology_change [REMOVED_NODE] event from cluster [#{cluster_name}] at [#{address}:#{port}], #{inspect(event)}"
    )

    terminate_control(cluster_name, removed, port)

    state
  end

  defp startup_control(cluster_name, host_id, address, rpc_address, port, data_center, options) do
    DynamicSupervisor.start_child(
      Controls,
      {Control, {self(), cluster_name, host_id, address, rpc_address, port, data_center, options}}
    )
    |> case do
      {:ok, _} ->
        Logger.debug(
          "Started control connection to cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
        )

        :ok

      {:error, {:already_started, _}} ->
        Logger.debug(
          "Already started up control connectionto cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
        )

        :ok

      err ->
        Logger.error(
          "Error starting control connection to cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}], #{inspect(err)}"
        )

        err
    end
  end

  defp terminate_control(cluster_name, rpc_address, port) do
    with [{host_id, pid}] <-
           Registry.select(ControlRegistry, [
             {{{cluster_name, :"$1"}, :"$2", %{rpc_address: rpc_address, port: port}}, [],
              [{{:"$1", :"$2"}}]}
           ]) do
      Logger.debug(
        "Terminating control connection from cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
      )

      DynamicSupervisor.terminate_child(Controls, pid)
    end
  end

  defp terminate_controls(cluster_name) do
    controls =
      Registry.select(ControlRegistry, [
        {{{cluster_name, :"$1"}, :"$2", %{rpc_address: :"$3", port: :"$4"}}, [],
         [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])

    Enum.each(controls, fn {host_id, pid, rpc_address, port} ->
      Logger.debug(
        "Terminating control connection from cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
      )

      DynamicSupervisor.terminate_child(Controls, pid)
    end)
  end

  defp inet_mod(:gen_tcp), do: :inet
  defp inet_mod(:ssl), do: :ssl

  defp process_page(%Xandra.Page{columns: [{_, _, "[applied]", _} | _], content: content}) do
    rows =
      content
      |> Enum.reject(&match?([false | _], &1))
      |> Enum.map(fn [_ | row] -> row end)

    %{rows: rows, num_rows: length(rows)}
  end

  defp process_page(%Xandra.Page{
         columns: [{_, _, "system.count" <> _, _} | _],
         content: [[count]]
       }) do
    %{rows: [[count]], num_rows: 1}
  end

  defp process_page(%Xandra.Page{columns: [{_, _, "count" <> _, _} | _], content: [[count]]}) do
    %{rows: [[count]], num_rows: 1}
  end

  defp process_page(%Xandra.Page{content: content}) do
    %{rows: content, num_rows: length(content)}
  end
end
