defmodule Xandra.Cluster.Application do
  use Application

  alias Xandra.Cluster.{
    DiscoveryRegistry,
    ControlRegistry,
    ConnectionRegistry,
    Discoveries,
    Controls,
    Connections
  }

  alias Xandra.Cluster.{Discovery, Control}
  alias Xandra.TableMetadata

  require Logger

  @schema_table :xandra_schema

  @impl true
  def start(_type, _args) do
    :ets.new(@schema_table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    children = [
      {Registry, keys: :unique, name: DiscoveryRegistry},
      {Registry, keys: :unique, name: ControlRegistry},
      {Registry, keys: :unique, name: ConnectionRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: Connections},
      {DynamicSupervisor, strategy: :one_for_one, name: Controls},
      {DynamicSupervisor, strategy: :one_for_one, name: Discoveries, intensity: 5, period: 3}
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: Xandra.Clusters)
  end

  def startup_cluster(cluster_name \\ :default, address, port, options) do
    Logger.debug("Starting up cluster [#{cluster_name}], at [#{address}:#{port}]")
    DynamicSupervisor.start_child(
      Discoveries,
      {Discovery, {cluster_name, address, port, options}}
    )
  end

  def startup_control(cluster_name, address, port, options) do
    DynamicSupervisor.start_child(Controls, {Control, {cluster_name, address, port, options}})
    |> case do
      {:ok, _} ->
        Logger.debug(
          "Starting up control connection [#{inspect(address)}:#{port}] to cluster [#{cluster_name}]"
        )

      {:error, {:already_started, _}} ->
        Logger.debug(
          "Already started up control connection [#{inspect(address)}:#{port}] to cluster [#{cluster_name}]"
        )

      err ->
        Logger.error(
          "Error starting up control connection [#{inspect(address)}:#{port}] for cluster [#{cluster_name}], #{inspect(err)}"
        )
    end
  end

  def terminate_control(cluster_name, address, port) do
    with [{pid, nil}] <- Registry.lookup(ControlRegistry, {cluster_name, address, port}) do
      Logger.warn(
        "Terminating control connection [#{inspect(address)}:#{port}] to cluster [#{cluster_name}]"
      )

      DynamicSupervisor.terminate_child(Controls, pid)
    end
  end

  def register_schema(%TableMetadata{cluster_name: cluster_name, keyspace_name: keyspace_name, table_name: table_name} = table) do
    :ets.insert(@schema_table, {{cluster_name, keyspace_name, table_name}, table})
  end
end
