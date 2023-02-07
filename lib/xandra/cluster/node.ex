defmodule Xandra.Cluster.Node do
  use GenServer

  alias DBConnection.Backoff
  alias __MODULE__

  require Logger

  @default_options [
    backoff_type: :rand_exp,
    backoff_min: 3_000,
    backoff_max: 30_000
  ]

  def get_conn(cluster_node) do
    GenServer.call(cluster_node, :conn)
  end

  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  @enforce_keys [:options, :node, :backoff, :conn]
  defstruct [:options, :node, :backoff, :conn]

  @impl true
  def init(options) do
    node = Keyword.fetch!(options, :nodes) |> hd()
    backoff = Backoff.new(Keyword.merge(@default_options, options))

    Process.flag(:trap_exit, true)
    {:ok, conn} = Xandra.start_link(options)

    {:ok, %Node{options: options, node: node, backoff: backoff, conn: conn}}
  end

  @impl true
  def handle_call(:conn, _from, %Node{conn: conn} = state) do
    {:reply, conn, state}
  end

  @impl true
  def handle_info({:EXIT, _, reason}, state) do
    Logger.warn("DBConnection exit detected on #{state.node}, #{inspect(reason)}")

    {wait, backoff} = Backoff.backoff(state.backoff)
    Process.send_after(self(), :connect, wait)

    {:noreply, %Node{state | backoff: backoff}}
  end

  @impl true
  def handle_info(:connect, %Node{options: options} = state) do
    Xandra.start_link(options)
    |> case do
      {:ok, conn} ->
        backoff = Backoff.reset(state.backoff)
        {:noreply, %Node{state | backoff: backoff, conn: conn}}

      err ->
        Logger.warn("Error starting connection pool to node #{state.node}, #{inspect(err)}")

        {wait, backoff} = Backoff.backoff(state.backoff)
        Process.send_after(self(), :connect, wait)
        {:noreply, %Node{state | backoff: backoff}}
    end
  end

  @impl true
  def terminate(reason, state) do
    Logger.debug("Disconnect from node #{state.node}, #{inspect(reason)}")

    state
  end
end
