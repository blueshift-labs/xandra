defmodule Xandra.Clusters.Control do
  @enforce_keys [
    :cluster_name,
    :host_id,
    :address,
    :rpc_address,
    :port,
    :data_center,
    :transport,
    :protocol_version,
    :options,
    :backoff,
    :attempts
  ]

  defstruct [
    :cluster_name,
    :host_id,
    :address,
    :rpc_address,
    :port,
    :data_center,
    :transport,
    :protocol_version,
    :options,
    :backoff,
    :attempts,
    :protocol_module,
    :socket,
    :error
  ]

  use Connection

  alias __MODULE__
  alias Xandra.{Frame, Simple, Connection.Utils}
  alias Xandra.Clusters.{ControlRegistry, Controls, Monitor, Peer}
  alias DBConnection.Backoff

  require Logger

  @backoff Backoff.new(backoff_type: :rand_exp, backoff_min: 1_000, backoff_max: 30_000)
  @max_attempts 10
  @connection_timeout 5_000
  @discover_interval 30_000
  @transport_options [packet: :raw, mode: :binary, keepalive: true, nodelay: true]

  def child_spec({cluster_name, host_id, address, rpc_address, port, data_center, options}) do
    %{
      id: __MODULE__,
      start: {
        __MODULE__,
        :start_link,
        [{cluster_name, host_id, address, rpc_address, port, data_center, options}]
      },
      type: :worker
    }
  end

  def start_link({cluster_name, host_id, address, rpc_address, port, data_center, options}) do
    Connection.start_link(
      __MODULE__,
      {cluster_name, host_id, address, rpc_address, port, data_center, options},
      name:
        {:via, Registry,
         {ControlRegistry, {cluster_name, host_id}, %{rpc_address: rpc_address, port: port}}}
    )
  end

  @impl true
  def init({cluster_name, host_id, address, rpc_address, port, data_center, options}) do
    encryption = Keyword.get(options, :encryption)
    transport = if encryption, do: :ssl, else: :gen_tcp
    protocol_version = Keyword.get(options, :protocol_version)

    Process.flag(:trap_exit, true)

    state = %Control{
      cluster_name: cluster_name,
      host_id: host_id,
      address: address,
      rpc_address: rpc_address,
      port: port,
      data_center: data_center,
      transport: transport,
      protocol_version: protocol_version,
      options: options,
      backoff: @backoff,
      attempts: 0
    }

    {:connect, :control, state}
  end

  @impl true
  def connect(
        _,
        %{
          cluster_name: cluster_name,
          host_id: host_id,
          rpc_address: rpc_address,
          port: port,
          attempts: attempts,
          error: err
        } = state
      )
      when attempts >= @max_attempts do
    Logger.error(
      "Failed to start control connection to cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}], #{inspect(err)}"
    )

    {:stop, err, state}
  end

  def connect(
        _,
        %{
          cluster_name: cluster_name,
          host_id: host_id,
          address: address,
          rpc_address: rpc_address,
          port: port,
          transport: transport,
          protocol_version: protocol_version,
          options: options,
          backoff: backoff,
          attempts: attempts
        } = state
      ) do
    Logger.debug(
      "Starting control connection to cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
    )

    transport_options = Keyword.put(@transport_options, :active, false)

    with {:ok, socket} <-
           transport.connect(address, port, transport_options, @connection_timeout),
         {:ok, {protocol_module, cql_version}} <-
           negotiate_protocol(transport, socket, protocol_version, state),
         :ok <-
           handshake_connection(transport, socket, protocol_module, cql_version, options, state),
         {:ok, _} <- Monitor.start_link(state) do
      Logger.debug(
        "Successfully started control connection to cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
      )

      {:ok,
       %{
         state
         | socket: socket,
           protocol_module: protocol_module,
           backoff: @backoff,
           attempts: 0,
           error: nil
       }, 0}
    else
      {:error, {:use_this_protocol_instead, _failed_protocol_version, protocol_version}} ->
        Logger.debug(
          "Switch to protocol #{protocol_version} for cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
        )

        {:backoff, 0, %{state | socket: nil, protocol_version: protocol_version}}

      err ->
        Logger.error(
          "Error connecting to cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}], #{inspect(err)}"
        )

        {wait, backoff} = Backoff.backoff(backoff)
        {:backoff, wait, %{state | backoff: backoff, attempts: attempts + 1, error: err}}
    end
  end

  @impl true
  def handle_info(
        :timeout,
        %{
          cluster_name: cluster_name,
          host_id: host_id,
          rpc_address: rpc_address,
          port: port
        } = state
      ) do
    Logger.debug(
      "Discovering system.local with cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
    )

    case discover_system_local(state) do
      :ok ->
        Process.send_after(self(), :discover_peers, 0)

        {:noreply, state, @discover_interval}

      err ->
        Logger.debug(
          "Error discovering system.local with cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}], #{inspect(err)}"
        )

        {:stop, err, state}
    end
  end

  def handle_info(
        :discover_peers,
        %{
          cluster_name: cluster_name,
          host_id: host_id,
          rpc_address: rpc_address,
          port: port,
          data_center: data_center,
          options: options
        } = state
      ) do
    Logger.debug(
      "Discovering peers with cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
    )

    with {:ok, system_peers} <- discover_system_peers(state),
         {:ok, cluster_status} <- discover_cluster_status(state) do
      cluster_status =
        cluster_status
        |> Enum.into(%{}, fn %{host_id: host_id, up: up} ->
          {host_id, up}
        end)

      peers =
        system_peers
        |> Enum.filter(&match?(%{data_center: ^data_center}, &1))
        |> Enum.filter(&cluster_status[&1[:host_id]])

      Logger.debug(
        "Discovered peers with cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}], [#{inspect(peers)}]"
      )

      Enum.each(peers, fn %{host_id: host_id, rpc_address: rpc_address} ->
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

      {:noreply, state}
    else
      err ->
        Logger.debug(
          "Error discovering peers with cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}], #{inspect(err)}"
        )

        {:stop, err, state}
    end
  end

  @impl true
  def terminate(
        reason,
        %{
          cluster_name: cluster_name,
          host_id: host_id,
          rpc_address: rpc_address,
          port: port,
          transport: transport,
          socket: socket
        } = state
      )
      when reason in [:normal, :shutdown] do
    Logger.debug(
      "Stopping control connection for cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
    )

    if socket do
      transport.close(socket)
    end

    state
  end

  def terminate({:shutdown, _}, state) do
    terminate(:shutdown, state)
  end

  def terminate(
        reason,
        %{
          cluster_name: cluster_name,
          host_id: host_id,
          rpc_address: rpc_address,
          port: port,
          transport: transport,
          socket: socket
        } = state
      ) do
    Logger.error(
      "Exiting control connection for cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}], #{inspect(reason)}"
    )

    if socket do
      transport.close(socket)
    end

    state
  end

  defp negotiate_protocol(
         transport,
         socket,
         protocol_version,
         %{cluster_name: cluster_name, host_id: host_id, rpc_address: rpc_address, port: port}
       ) do
    Logger.debug(
      "Negotiating protocol with cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
    )

    with {:ok, %{"CQL_VERSION" => [cql_version | _]} = _supported_options, protocol_module} <-
           Utils.request_options(transport, socket, protocol_version) do
      Logger.debug(
        "Using protocol #{protocol_module}, cql #{cql_version} with cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
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
         %{cluster_name: cluster_name, host_id: host_id, rpc_address: rpc_address, port: port}
       ) do
    Logger.debug(
      "Handshaking control connection with cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
    )

    requested_options = %{"CQL_VERSION" => cql_version}
    Utils.startup_connection(transport, socket, requested_options, protocol_module, nil, options)
  end

  defp discover_system_local(%{
         cluster_name: cluster_name,
         host_id: host_id,
         port: port,
         transport: transport,
         protocol_module: protocol_module,
         socket: socket
       }) do
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
      peer = Peer.from_system_local(cluster_name, port, system_local)

      Registry.update_value(ControlRegistry, {cluster_name, host_id}, fn _ -> peer end)

      :ok
    end
  end

  defp discover_system_peers(%{
         cluster_name: cluster_name,
         host_id: host_id,
         rpc_address: rpc_address,
         port: port,
         transport: transport,
         socket: socket,
         protocol_module: protocol_module
       }) do
    Logger.debug(
      "Discovering system.peers with cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
    )

    query = %Simple{
      statement: "SELECT * FROM system.peers",
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

      {:ok, Enum.to_list(page)}
    end
  end

  defp discover_cluster_status(%{
         cluster_name: cluster_name,
         host_id: host_id,
         rpc_address: rpc_address,
         port: port,
         transport: transport,
         socket: socket,
         protocol_module: protocol_module
       }) do
    Logger.debug(
      "Discovering system.cluster_status with cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
    )

    query = %Simple{
      statement: "SELECT * FROM system.cluster_status",
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

      {:ok, Enum.to_list(page)}
    end
  end

  def startup_control(cluster_name, host_id, address, rpc_address, port, data_center, options) do
    DynamicSupervisor.start_child(
      Controls,
      {Control, {cluster_name, host_id, address, rpc_address, port, data_center, options}}
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
end
