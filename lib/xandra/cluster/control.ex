defmodule Xandra.Cluster.Control do
  @enforce_keys [
    :cluster_name,
    :address,
    :port,
    :transport,
    :protocol_version,
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
    :options,
    :attempts,
    :state,
    :protocol_module,
    :cql_version,
    :socket,
    :error
  ]

  use Connection

  alias __MODULE__
  alias Xandra.{Frame, Simple, Connection.Utils}

  alias Xandra.Cluster.{ControlRegistry, ConnectionRegistry, Connections}

  require Logger

  @max_attempts 10
  @backoff_timeout 3_000
  @connection_timeout 5_000
  @request_interval 5_000
  @transport_options [packet: :raw, mode: :binary, keepalive: true, nodelay: true]

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
    Connection.start_link(
      __MODULE__,
      {cluster_name, address, port, options},
      name: {:via, Registry, {ControlRegistry, {cluster_name, address, port}}}
    )
  end

  @impl true
  def init({cluster_name, address, port, options}) do
    encryption = Keyword.get(options, :encryption)
    transport = if encryption, do: :ssl, else: :gen_tcp
    protocol_version = Keyword.get(options, :protocol_version)

    Process.flag(:trap_exit, true)

    state = %Control{
      cluster_name: cluster_name,
      address: address,
      port: port,
      transport: transport,
      protocol_version: protocol_version,
      options: options,
      attempts: 0,
      state: :init
    }

    Logger.debug(
      "Starting control connection to cluster [#{cluster_name}] at [#{inspect(address)}:#{port}]"
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
      "Failed to start control connection to cluster [#{cluster_name}] at [#{inspect(address)}:#{port}], #{inspect(err)}"
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
    Logger.debug("Connecting to cluster [#{cluster_name}] at [#{inspect(address)}:#{port}]")
    transport_options = Keyword.put(@transport_options, :active, false)

    with {:ok, socket} <-
           transport.connect(address, port, transport_options, @connection_timeout) do
      Logger.debug(
        "Successfully connected to cluster [#{cluster_name}] at [#{inspect(address)}:#{port}]"
      )

      {:ok, %{state | socket: socket, attempts: 0, error: nil}, 0}
    else
      {:error, err} ->
        Logger.error(
          "Error connecting to cluster [#{cluster_name}] at [#{inspect(address)}:#{port}], #{inspect(err)}"
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
    with {:ok, {protocol_module, cql_version}} <-
           negotiate_protocol(transport, socket, protocol_version, state),
         :ok <-
           startup_connection(transport, socket, protocol_module, cql_version, options, state),
         {:ok, system_local} <-
           request_system_local(transport, socket, protocol_module, state),
         {:ok, _} <- start_connection_pool(cluster_name, address, port, options, state) do
      
      # TODO report system_local
      {:noreply,
       %{state | protocol_module: protocol_module, cql_version: cql_version, state: :started},
       @request_interval}
    else
      {:error, {:use_this_protocol_instead, _failed_protocol_version, protocol_version}} ->
        # reconnect right away
        Logger.debug("Switch to protocol #{protocol_version} for cluster [#{cluster_name}]")
        transport.close(socket)
        {:connect, :init, %{state | socket: nil, protocol_version: protocol_version}}

      {:error, err} ->
        Logger.error(
          "Failed to handshake control connection for cluster [#{cluster_name}] at [#{inspect(address)}:#{port}], #{inspect(err)}"
        )

        {:disconnect, {:error, err}, state}
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
      "Disconnecting control connection from cluster [#{cluster_name}] at [#{inspect(address)}:#{port}], #{inspect(err)}"
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
      "Stopping control connection for cluster [#{cluster_name}] at [#{inspect(address)}:#{port}]"
    )

    terminate_connection_pool(cluster_name, address, port)

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
      "Exiting control connection for cluster [#{cluster_name}] at [#{inspect(address)}:#{port}], #{inspect(reason)}"
    )

    terminate_connection_pool(cluster_name, address, port)

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
    Logger.debug(
      "Negotiating protocol with cluster [#{cluster_name}] at [#{inspect(address)}:#{port}]"
    )

    with {:ok, %{"CQL_VERSION" => [cql_version | _]} = _supported_options, protocol_module} <-
           Utils.request_options(transport, socket, protocol_version) do
      Logger.debug(
        "Using protocol #{protocol_module}, cql #{cql_version} with cluster [#{cluster_name}] at [#{inspect(address)}:#{port}]"
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
    Logger.debug(
      "Starting up connection with cluster [#{cluster_name}] at [#{inspect(address)}:#{port}]"
    )

    requested_options = %{"CQL_VERSION" => cql_version}

    Utils.startup_connection(transport, socket, requested_options, protocol_module, nil, options)
  end

  defp request_system_local(
         transport,
         socket,
         protocol_module,
         %{cluster_name: cluster_name, address: address, port: port}
       ) do
    Logger.debug(
      "Requesting system.local with cluster [#{cluster_name}] at [#{inspect(address)}:#{port}]"
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
      {%Xandra.Page{} = page, _warnings} = protocol_module.decode_response(frame, query)

      [system_local] = Enum.to_list(page)
      {:ok, system_local}
    end
  end

  defp start_connection_pool(_cluster_name, _address, _port, _options, %{state: :started}), do: {:ok, nil}

  defp start_connection_pool(cluster_name, address, port, options, _state) do
    Logger.debug("Starting connection pool for cluster [#{cluster_name}], at [#{inspect(address)}:#{port}]")

    options = options
    |> Keyword.merge([nodes: ["#{:inet.ntoa(address)}"], port: port])
    |> Keyword.put(:name, {:via, Registry, {ConnectionRegistry, {cluster_name, address, port}}})

    child_spec = %{
      id: {cluster_name, address, port},
      start: {
        Xandra,
        :start_link,
        [options]
      },
      restart: :permanent,
      shutdown: :infinity,
      type: :worker
    }

    DynamicSupervisor.start_child(Connections, child_spec)
  end

  defp terminate_connection_pool(cluster_name, address, port) do
    with [{pid, nil}] <- Registry.lookup(ConnectionRegistry, {cluster_name, address, port}) do
      Logger.debug("Terminating connection pool for cluster [#{cluster_name}], at [#{inspect(address)}:#{port}]")

      DynamicSupervisor.terminate_child(Connections, pid) 
    end
  end
end
