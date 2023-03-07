defmodule Xandra.Clusters.Monitor do
  @enforce_keys [
    :cluster_name,
    :host_id,
    :address,
    :rpc_address,
    :port,
    :options,
    :backoff
  ]

  defstruct [
    :cluster_name,
    :host_id,
    :address,
    :rpc_address,
    :port,
    :options,
    :backoff
  ]

  use GenServer

  alias DBConnection.Backoff
  alias Xandra.Clusters.ConnectionRegistry
  alias __MODULE__

  require Logger

  @backoff Backoff.new(backoff_type: :rand_exp, backoff_min: 1_000, backoff_max: 30_000)

  def start_link(options) do
    GenServer.start_link(Monitor, options)
  end

  @impl true
  def init(%{
        cluster_name: cluster_name,
        host_id: host_id,
        address: address,
        rpc_address: rpc_address,
        port: port,
        options: options
      }) do
    Process.flag(:trap_exit, true)

    state = %Monitor{
      cluster_name: cluster_name,
      host_id: host_id,
      address: address,
      rpc_address: rpc_address,
      port: port,
      options: options,
      backoff: @backoff
    }

    Process.send_after(self(), :connect, 0)
    {:ok, state}
  end

  @impl true
  def handle_info(
        :connect,
        %{
          cluster_name: cluster_name,
          host_id: host_id,
          rpc_address: rpc_address,
          port: port,
          backoff: backoff
        } = state
      ) do
    Logger.debug(
      "Starting connection pool for cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
    )

    case start_pool(state) do
      {:ok, _} ->
        {:noreply, state}

      err ->
        Logger.error(
          "Error starting connection pool for cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}], #{inspect(err)}"
        )

        {wait, backoff} = Backoff.backoff(backoff)
        Process.send_after(self(), :connect, wait)
        {:noreply, %{state | backoff: backoff}}
    end
  end

  def handle_info(
        {:EXIT, _pid, {err, _info}},
        %{
          cluster_name: cluster_name,
          host_id: host_id,
          rpc_address: rpc_address,
          port: port,
          backoff: backoff
        } = state
      ) do
    Logger.error(
      "Connection pool exited for cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}], #{inspect(err)}"
    )

    {wait, backoff} = Backoff.backoff(backoff)
    Process.send_after(self(), :connect, wait)
    {:noreply, %{state | backoff: backoff}}
  end

  @impl true
  def terminate(
        reason,
        %{
          cluster_name: cluster_name,
          host_id: host_id,
          rpc_address: rpc_address,
          port: port
        } = state
      )
      when reason in [:normal, :shutdown] do
    Logger.debug(
      "Stop monitoring connection for cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}]"
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
          host_id: host_id,
          rpc_address: rpc_address,
          port: port
        } = state
      ) do
    Logger.error(
      "Exiting monitoring connection for cluster [#{cluster_name}] at [#{rpc_address}:#{port}]@[#{host_id}], #{inspect(reason)}"
    )

    state
  end

  defp start_pool(%{
         cluster_name: cluster_name,
         host_id: host_id,
         address: address,
         rpc_address: rpc_address,
         port: port,
         options: options
       }) do
    address =
      case address do
        address when is_tuple(address) -> :inet.ntoa(address)
        _ -> address
      end

    options
    |> Keyword.drop([:address])
    |> Keyword.merge(nodes: ["#{address}:#{port}"], port: port)
    |> Keyword.put(
      :name,
      {:via, Registry, {ConnectionRegistry, {cluster_name, host_id}, {rpc_address, port}}}
    )
    |> Xandra.start_link()
  end
end
