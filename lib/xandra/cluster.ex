defmodule Xandra.Cluster do
  alias Xandra.{Prepared, Batch, RetryStrategy, ConnectionError}
  alias Xandra.Clusters.{ConnectionRegistry, Cluster, Peer}

  require Logger

  @type cluster :: GenServer.server()

  start_link_opts_schema = [
    cluster_name: [
      type: :string,
      default: "default",
      doc: false
    ],
    address: [
      type: :string,
      required: true,
      doc: false
    ],
    port: [
      type: :non_neg_integer,
      default: 9042,
      doc: false
    ],
    load_balancing: [
      type: {:list, {:in, [:random, :rack_aware, :token_aware]}},
      default: [:random],
      doc: false
    ],
    keyspace: [
      type: :string,
      doc: false
    ],
    name: [
      type: :any,
      doc: false
    ]
  ]

  @start_link_opts_schema NimbleOptions.new!(start_link_opts_schema)
  @start_link_opts_schema_keys Keyword.keys(start_link_opts_schema)

  def start_link(options) do
    {cluster_opts, _} = Keyword.split(options, @start_link_opts_schema_keys)
    cluster_opts = NimbleOptions.validate!(cluster_opts, @start_link_opts_schema)
    options = Keyword.merge(options, cluster_opts)

    {name, options} = Keyword.split(options, [:name])
    Connection.start_link(Cluster, options, name)
  end

  def child_spec(options) do
    %{
      id: __MODULE__,
      type: :worker,
      start: {__MODULE__, :start_link, [options]}
    }
  end

  @doc """
  Returns a stream of pages.

  When streaming pages through a cluster, the streaming is done
  from a single node, that is, this function just calls out to
  `Xandra.stream_pages!/4` after choosing a node appropriately.

  All options are forwarded to `Xandra.stream_pages!/4`, including
  retrying options.
  """
  @spec stream_pages!(
          cluster,
          Xandra.statement() | Prepared.t(),
          Xandra.values(),
          keyword
        ) ::
          Enumerable.t()
  def stream_pages!(cluster, query, params, options \\ []) do
    with_conn(cluster, options, &Xandra.stream_pages!(&1, query, params, options))
  end

  @doc """
  Same as `Xandra.prepare/3`.

  Preparing a query through `Xandra.Cluster` will prepare it only on one node,
  according to the load balancing strategy chosen in `start_link/1`. To prepare
  and execute a query on the same node, you could use `run/3`:

      Xandra.Cluster.run(cluster, fn conn ->
        # "conn" is the pool of connections for a specific node.
        prepared = Xandra.prepare!(conn, "SELECT * FROM system.local")
        Xandra.execute!(conn, prepared, _params = [])
      end)

  Thanks to the prepared query cache, we can always reprepare the query and execute
  it because after the first time (on each node) the prepared query will be fetched
  from the cache. However, if a prepared query is unknown on a node, Xandra will
  prepare it on that node on the fly, so we can simply do this as well:

      prepared = Xandra.Cluster.prepare!(cluster, "SELECT * FROM system.local")
      Xandra.Cluster.execute!(cluster, prepared, _params = [])

  Note that this goes through the cluster twice, so there's a high chance that
  the query will be prepared on one node and then executed on another node.
  This is however useful if you want to use the `:retry_strategy` option in
  `execute!/4`: in the `run/3` example above, if you use `:retry_strategy` with
  `Xandra.execute!/3`, the query will be retried on the same pool of connections
  to the same node. `execute!/4` will retry queries going through the cluster
  again instead.
  """
  @spec prepare(cluster, Xandra.statement(), keyword) ::
          {:ok, Prepared.t()} | {:error, Xandra.error()}
  def prepare(cluster, statement, options \\ []) when is_binary(statement) do
    with_conn(cluster, options, &Xandra.prepare(&1, statement, options))
  end

  @doc """
  Same as `prepare/3` but raises in case of errors.

  If the function is successful, the prepared query is returned directly
  instead of in an `{:ok, prepared}` tuple like in `prepare/3`.
  """
  @spec prepare!(cluster, Xandra.statement(), keyword) :: Prepared.t() | no_return
  def prepare!(cluster, statement, options \\ []) do
    case prepare(cluster, statement, options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Same as `execute/4` but with optional arguments.
  """
  @spec execute(cluster, Xandra.statement() | Prepared.t(), Xandra.values()) ::
          {:ok, Xandra.result()} | {:error, Xandra.error()}
  @spec execute(cluster, Xandra.Batch.t(), keyword) ::
          {:ok, Xandra.Void.t()} | {:error, Xandra.error()}
  def execute(cluster, query, params_or_options \\ [])

  def execute(cluster, statement, params) when is_binary(statement) do
    execute(cluster, statement, params, _options = [])
  end

  def execute(cluster, %Prepared{} = prepared, params) do
    execute(cluster, prepared, params, _options = [])
  end

  def execute(cluster, %Batch{} = batch, options) when is_list(options) do
    with_conn_and_retrying(cluster, options, &Xandra.execute(&1, batch, options))
  end

  @doc """
  Same as `execute/3` but returns the result directly or raises in case of errors.
  """
  @spec execute!(cluster, Xandra.statement() | Prepared.t(), Xandra.values()) ::
          Xandra.result() | no_return
  @spec execute!(cluster, Xandra.Batch.t(), keyword) ::
          Xandra.Void.t() | no_return
  def execute!(cluster, query, params_or_options \\ []) do
    case execute(cluster, query, params_or_options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Executes a query on a node in the cluster.

  This function executes a query on a node in the cluster. The node is chosen based
  on the load balancing strategy given in `start_link/1`.

  Supports the same options as `Xandra.execute/4`. In particular, the `:retry_strategy`
  option is cluster-aware, meaning that queries are retried on possibly different nodes
  in the cluster.
  """
  @spec execute(cluster, Xandra.statement() | Prepared.t(), Xandra.values(), keyword) ::
          {:ok, Xandra.result()} | {:error, Xandra.error()}
  def execute(cluster, query, params, options) do
    with_conn_and_retrying(cluster, options, &Xandra.execute(&1, query, params, options))
  end

  @doc """
  Same as `execute/4` but returns the result directly or raises in case of errors.
  """
  @spec execute!(cluster, Xandra.statement() | Prepared.t(), Xandra.values(), keyword) ::
          Xandra.result() | no_return
  def execute!(cluster, query, params, options) do
    case execute(cluster, query, params, options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Runs a function with a given connection.

  The connection that is passed to `fun` is a Xandra connection, not a
  cluster. This means that you should call `Xandra` functions on it.
  Since the connection is a single connection, it means that it's a connection
  to a specific node, so you can do things like prepare a query and then execute
  it because you can be sure it's prepared on the same node where you're
  executing it.

  ## Examples

      query = "SELECT * FROM system_schema.keyspaces"

      Xandra.Cluster.run(cluster, fn conn ->
        prepared = Xandra.prepare!(conn, query)
        Xandra.execute!(conn, prepared, _params = [])
      end)

  """
  @spec run(cluster, keyword, (Xandra.conn() -> result)) :: result when result: var
  def run(cluster, options \\ [], fun) do
    with_conn_and_retrying(cluster, options, &Xandra.run(&1, options, fun))
  end

  defp with_conn_and_retrying(cluster, options, fun) do
    RetryStrategy.run_with_retrying(options, fn -> with_conn(cluster, options, fun) end)
  end

  defp with_conn(cluster, options, fun) do
    cluster_name = Keyword.fetch!(options, :cluster_name)
    load_balancing = Keyword.fetch!(options, :load_balancing)

    pools =
      Registry.select(ConnectionRegistry, [
        {{{cluster_name, :"$1"}, :"$2", {:"$3", :"$4"}}, [],
         [{{cluster_name, :"$1", :"$2", :"$3", :"$4"}}]}
      ])

    pool = select_pool(load_balancing, pools, options)

    case pool do
      nil ->
        action = "checkout from cluster #{inspect(cluster)}"
        {:error, ConnectionError.new(action, {:cluster, :not_connected})}

      _ ->
        fun.(pool)
    end
  end

  defp select_pool(_load_balancing, [], _options), do: nil

  defp select_pool([], pools, _options) do
    {_, _, pool, _, _} = Enum.random(pools)
    pool
  end

  defp select_pool([:random | _], pools, options) do
    select_pool([], pools, options)
  end

  defp select_pool([:rack_aware | _] = load_balancing, pools, options) do
    {rack, options} = Keyword.pop(options, :rack)
    select_pool(load_balancing, pools, rack, options)
  end

  defp select_pool([:token_aware | _] = load_balancing, pools, options) do
    {token, options} = Keyword.pop(options, :token)
    select_pool(load_balancing, pools, token, options)
  end

  defp select_pool([_ | load_balancing], pools, options) do
    select_pool(load_balancing, pools, options)
  end

  defp select_pool([:rack_aware | load_balancing], pools, nil, options) do
    select_pool(load_balancing, pools, options)
  end

  defp select_pool([:rack_aware | load_balancing], pools, rack, options) do
    rack_pools =
      Enum.filter(pools, fn {cluster_name, host_id, _pid, _rpc_address, _port} ->
        Registry.lookup(ControlRegistry, {cluster_name, host_id})
        |> case do
          [{_, %Peer{rack: ^rack}}] -> true
          _ -> false
        end
      end)

    case rack_pools do
      [] ->
        select_pool(load_balancing, pools, options)

      _ ->
        select_pool(load_balancing, rack_pools, options)
    end
  end

  defp select_pool([:token_aware | load_balancing], pools, nil, options) do
    select_pool(load_balancing, pools, options)
  end

  defp select_pool([:token_aware | load_balancing], pools, _token, options) do
    token_pools =
      Enum.filter(pools, fn {cluster_name, host_id, _pid, _rpc_address, _port} ->
        Registry.lookup(ControlRegistry, {cluster_name, host_id})
        |> case do
          [{_, %Peer{}}] -> true
          _ -> false
        end
      end)

    case token_pools do
      [] ->
        select_pool(load_balancing, pools, options)

      _ ->
        select_pool(load_balancing, token_pools, options)
    end
  end
end
