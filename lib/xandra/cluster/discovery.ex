defmodule Xandra.Cluster.Discovery do
  @enforce_keys [
    :cluster_name,
    :address,
    :port,
    :transport,
    :protocol_version,
    :load_balancing,
    :keyspace,
    :options,
    :attempts,
    :state
  ]

  defstruct [
    :cluster_name,
    :address,
    :port,
    :transport,
    :protocol_version,
    :load_balancing,
    :keyspace,
    :options,
    :attempts,
    :state,
    :protocol_module,
    :cql_version,
    :data_center,
    :socket,
    :error,
    buffer: <<>>
  ]

  use Connection

  alias __MODULE__
  alias Xandra.{Frame, Simple, TableMetadata, Connection.Utils}

  alias Xandra.Cluster.Application, as: Clusters
  alias Xandra.Cluster.{DiscoveryRegistry, StatusChange, TopologyChange}

  require Logger

  @max_attempts 5
  @backoff_timeout 3_000
  @connection_timeout 5_000
  @request_interval 5_000
  @transport_options [packet: :raw, mode: :binary, keepalive: true, nodelay: true]

  @system_cluster_status_query %Simple{
    statement: "SELECT * FROM system.cluster_status",
    values: [],
    default_consistency: :one
  }

  def child_spec({cluster_name, address, port, options}) do
    %{
      id: cluster_name,
      start: {
        __MODULE__,
        :start_link,
        [{cluster_name, address, port, options}]
      },
      restart: :permanent,
      shutdown: :infinity,
      type: :worker
    }
  end

  def start_link({cluster_name, address, port, options}) do
    name = {:via, Registry, {DiscoveryRegistry, {cluster_name, address, port}}}
    Connection.start_link(__MODULE__, {cluster_name, address, port, options}, name: name)
  end

  @impl true
  def init({cluster_name, address, port, options}) do
    load_balancing = Keyword.get(options, :load_balancing)
    keyspace = Keyword.get(options, :keyspace)
    encryption = Keyword.get(options, :encryption)
    transport = if encryption, do: :ssl, else: :gen_tcp
    protocol_version = Keyword.get(options, :protocol_version)

    Process.flag(:trap_exit, true)

    state = %Discovery{
      cluster_name: cluster_name,
      address: address,
      port: port,
      transport: transport,
      protocol_version: protocol_version,
      load_balancing: load_balancing,
      keyspace: keyspace,
      options: options,
      attempts: 0,
      state: :init
    }

    Logger.debug(
      "Starting discovery connection to cluster [#{cluster_name}], at [#{address}:#{port}]"
    )

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
      "Failed to start discovery connection to cluster [#{cluster_name}], at [#{address}:#{port}], #{inspect(err)}"
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
          attempts: attempts
        } = state
      ) do
    Logger.debug("Connecting to cluster [#{cluster_name}], at [#{address}:#{port}]")
    transport_options = Keyword.put(@transport_options, :active, false)

    with {:ok, socket} <-
           transport.connect(to_charlist(address), port, transport_options, @connection_timeout) do
      Logger.debug("Successfully connected to cluster [#{cluster_name}], at [#{address}:#{port}]")
      {:ok, %{state | socket: socket, attempts: 0, error: nil}, 0}
    else
      {:error, err} ->
        Logger.error(
          "Error connecting to cluster [#{cluster_name}], at [#{address}:#{port}], #{inspect(err)}"
        )

        {:backoff, @backoff_timeout * 2 ** attempts,
         %{state | attempts: attempts + 1, error: err}}
    end
  end

  @impl true
  def handle_info(
        :timeout,
        %{
          cluster_name: cluster_name,
          address: address,
          port: port,
          transport: transport,
          protocol_version: protocol_version,
          options: options,
          socket: socket
        } = state
      ) do
    Logger.debug("Discovering cluster [#{cluster_name}], at [#{address}:#{port}]")

    with {:ok, {protocol_module, cql_version}} <-
           negotiate_protocol(transport, socket, protocol_version, state),
         :ok <-
           startup_connection(transport, socket, protocol_module, cql_version, options, state),
         {:ok, %{data_center: data_center}} <-
           discover_system_local(transport, socket, protocol_module, state),
         :ok <- discover_system_schema(transport, socket, protocol_module, state),
         :ok <-
           register_to_events(transport, socket, protocol_module, state),
         :ok <-
           request_system_peers(transport, socket, protocol_module, state) do
      state = %{
        state
        | protocol_module: protocol_module,
          cql_version: cql_version,
          data_center: data_center,
          state: :started
      }

      {:noreply, state}
    else
      {:error, {:use_this_protocol_instead, _failed_protocol_version, protocol_version}} ->
        # reconnect right away
        Logger.debug("Switch to protocol #{protocol_version} for cluster [#{cluster_name}]")
        transport.close(socket)
        {:connect, :init, %{state | socket: nil, protocol_version: protocol_version}}

      {:error, err} ->
        Logger.error(
          "Failed to handshake discovery connection for cluster [#{cluster_name}], at [#{address}:#{port}], #{inspect(err)}"
        )

        {:stop, {:error, err}, state}
    end
  end

  @impl true
  def handle_info({kind, _}, state) when kind in [:tcp_passive, :ssl_passive],
    do: {:noreply, state}

  @impl true
  def handle_info({kind, _}, state) when kind in [:tcp_closed, :ssl_closed],
    do: {:disconnect, {:error, :closed}, state}

  @impl true
  def handle_info({kind, _, err}, state) when kind in [:tcp_error, :ssl_error],
    do: {:disconnect, {:error, err}, state}

  @impl true
  def handle_info(
        {kind, _socket, message},
        %{cluster_name: cluster_name, address: address, port: port, buffer: buffer} = state
      )
      when kind in [:tcp, :ssl] do
    Logger.debug(
      "Receiving discovery message from cluster [#{cluster_name}], at [#{address}:#{port}]"
    )

    handle_message(%{state | buffer: buffer <> message})
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
    Logger.error(
      "Disconnecting discovery connection from cluster [#{cluster_name}], at [#{address}:#{port}], #{inspect(err)}"
    )

    if socket do
      transport.close(socket)
    end

    {:stop, err, state}
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
    Logger.debug(
      "Stopping discovery connection for cluster [#{cluster_name}], at [#{address}:#{port}]"
    )

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
      "Exiting discovery connection for cluster [#{cluster_name}], at [#{address}:#{port}], #{inspect(reason)}"
    )

    state
  end

  defp negotiate_protocol(
         _transport,
         _socket,
         _protocol_version,
         %{protocol_module: protocol_module, cql_version: cql_version}
       )
       when not is_nil(protocol_module) and not is_nil(cql_version),
       do: {:ok, {protocol_module, cql_version}}

  defp negotiate_protocol(
         transport,
         socket,
         protocol_version,
         %{cluster_name: cluster_name, address: address, port: port}
       ) do
    Logger.debug("Negotiating protocol with cluster [#{cluster_name}], at [#{address}:#{port}]")

    with {:ok, %{"CQL_VERSION" => [cql_version | _]} = _supported_options, protocol_module} <-
           Utils.request_options(transport, socket, protocol_version) do
      Logger.debug(
        "Using protocol #{protocol_module}, cql #{cql_version} with cluster [#{cluster_name}], at [#{address}:#{port}]"
      )

      {:ok, {protocol_module, cql_version}}
    end
  end

  defp startup_connection(_transport, _socket, _protocol_module, _cql_version, _options, %{
         state: :started
       }),
       do: :ok

  defp startup_connection(
         transport,
         socket,
         protocol_module,
         cql_version,
         options,
         %{cluster_name: cluster_name, address: address, port: port}
       ) do
    Logger.debug("Starting up connection with cluster [#{cluster_name}], at [#{address}:#{port}]")
    requested_options = %{"CQL_VERSION" => cql_version}
    Utils.startup_connection(transport, socket, requested_options, protocol_module, nil, options)
  end

  defp discover_system_local(
         transport,
         socket,
         protocol_module,
         %{cluster_name: cluster_name, address: address, port: port, data_center: nil}
       ) do
    Logger.debug(
      "Discovering system.local with cluster [#{cluster_name}], at [#{address}:#{port}]"
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
           Utils.recv_frame(transport, socket, protocol_format, _compressor = nil),
         {%Xandra.Page{} = page, _warnings} <-
           protocol_module.decode_response(%{frame | atom_keys?: true}, query) do
      [system_local] = Enum.to_list(page)
      {:ok, system_local}
    end
  end

  defp discover_system_local(_transport, _socket, _protocol_module, state), do: {:ok, state}

  defp discover_system_schema(_transport, _socket, _protocol_module, %{state: :started}), do: :ok

  defp discover_system_schema(_transport, _socket, _protocol_module, %{keyspace: nil}), do: :ok

  defp discover_system_schema(transport, socket, protocol_module, %{
         cluster_name: cluster_name,
         address: address,
         port: port,
         load_balancing: :token_aware,
         keyspace: keyspace
       } = state) do
    Logger.debug(
      "Discovering system schema with cluster [#{cluster_name}], at [#{address}:#{port}]"
    )

    with {:ok, tables} <- discover_keyspace_tables(transport, socket, protocol_module, state) do
      Enum.each(tables, &discover_keyspace_table(transport, socket, protocol_module, &1, state))
    end
  end

  defp discover_keyspace_tables(transport, socket, protocol_module, %{cluster_name: cluster_name, address: address, port: port, keyspace: keyspace}) do
    Logger.debug(
      "Discovering keyspace tables for keyspace [#{keyspace}] with cluster [#{cluster_name}], at [#{address}:#{port}]"
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

      tables = page
      |> Enum.to_list()
      |> Enum.map(& &1[:table_name])
      {:ok, tables}
    end
  end

  defp discover_keyspace_table(transport, socket, protocol_module, table, %{cluster_name: cluster_name, address: address, port: port, keyspace: keyspace}) do
    Logger.debug(
      "Discovering table columns for table [#{keyspace}.#{table}] with cluster [#{cluster_name}], at [#{address}:#{port}]"
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

  defp discover_system_schema(_transport, _socket, _protocol_module, _state), do: :ok

  defp register_to_events(_transport, _socket, _protocol_module, %{state: :started}), do: :ok

  defp register_to_events(transport, socket, protocol_module, %{
         cluster_name: cluster_name,
         address: address,
         port: port
       }) do
    Logger.debug("Registering to events with cluster [#{cluster_name}], at [#{address}:#{port}]")

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

  defp request_system_peers(
         transport,
         socket,
         protocol_module,
         %{cluster_name: cluster_name, address: address, port: port}
       ) do
    Logger.debug(
      "Requesting system.cluster_status with cluster [#{cluster_name}], at [#{address}:#{port}]"
    )

    payload =
      Frame.new(:query, _options = [])
      |> protocol_module.encode_request(@system_cluster_status_query)
      |> Frame.encode(protocol_module)

    transport.send(socket, payload)
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

  defp handle_message(
         %{
           protocol_module: protocol_module,
           buffer: buffer,
           data_center: data_center,
           cluster_name: cluster_name,
           address: address,
           port: port,
           options: options
         } = state
       ) do
    case decode_frame(buffer) do
      {%Frame{kind: :result} = frame, rest} ->
        {page, _warnings} =
          protocol_module.decode_response(
            %{frame | atom_keys?: true},
            @system_cluster_status_query
          )

        {ups, downs} =
          page
          |> Enum.to_list()
          |> Enum.filter(&(&1[:dc] == data_center))
          |> Enum.split_with(& &1[:up])

        ups = Enum.map(ups, & &1[:peer])
        downs = Enum.map(downs, & &1[:peer])

        Logger.debug(
          "Received system.cluster_status from cluster [#{cluster_name}], at [#{address}:#{port}], ups: #{inspect(ups)}, downs: #{inspect(downs)}"
        )

        # start up connections for up peer
        Enum.each(ups, fn up ->
          Clusters.startup_control(cluster_name, up, port, options)
        end)

        # terminate connections for down peer
        Enum.each(downs, fn down ->
          Clusters.terminate_control(cluster_name, down, port)
        end)

        Process.send_after(self(), :timeout, @request_interval)
        handle_message(%{state | buffer: rest})

      {frame, rest} ->
        event = protocol_module.decode_response(%{frame | atom_keys?: true})
        state = handle_event(event, state)
        handle_message(%{state | buffer: rest})

      :error ->
        {:noreply, state}
    end
  end

  defp handle_event(
         %StatusChange{effect: "UP"} = event,
         %{
           cluster_name: cluster_name,
           address: address,
           port: port
         } = state
       ) do
    Logger.debug(
      "Received status_change [UP] event from cluster [#{cluster_name}], at [#{address}:#{port}], #{inspect(event)}, ignored"
    )

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
      "Received status_change [DOWN] event from cluster [#{cluster_name}], at [#{address}:#{port}], #{inspect(event)}"
    )

    Clusters.terminate_control(cluster_name, down, port)

    state
  end

  defp handle_event(
         %TopologyChange{effect: "NEW_NODE"} = event,
         %{
           cluster_name: cluster_name,
           address: address,
           port: port
         } = state
       ) do
    Logger.debug(
      "Received topology_change [NEW_NODE] event from cluster [#{cluster_name}], at [#{address}:#{port}], #{inspect(event)}, ignored"
    )

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
      "Received topology_change [MOVED_NODE] event from cluster [#{cluster_name}], at [#{address}:#{port}], #{inspect(event)}, ignored"
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
      "Received topology_change [REMOVED_NODE] event from cluster [#{cluster_name}], at [#{address}:#{port}], #{inspect(event)}"
    )

    Clusters.terminate_control(cluster_name, removed, port)

    state
  end

  defp inet_mod(:gen_tcp), do: :inet
  defp inet_mod(:ssl), do: :ssl
end
