defmodule Xandra.Clusters.Application do
  use Application

  alias Xandra.Clusters.{ControlRegistry, ConnectionRegistry, Controls}
  alias Xandra.TableMetadata

  require Logger

  @schema_table :xandra_schema
  @cluster_info :xandra_cluster_info

  @impl true
  def start(_type, _args) do
    :ets.new(@schema_table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@cluster_info, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    children = [
      {Registry, keys: :unique, name: ControlRegistry},
      {Registry, keys: :unique, name: ConnectionRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: Controls}
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: Xandra.Clusters)
  end

  def register_schema(
        %TableMetadata{
          cluster_name: cluster_name,
          keyspace_name: keyspace_name,
          table_name: table_name
        } = table
      ) do
    :ets.insert(@schema_table, {{cluster_name, keyspace_name, table_name}, table})
  end

  def lookup_schema(cluster_name, keyspace_name, table_name) do
    case :ets.lookup(@schema_table, {cluster_name, keyspace_name, table_name}) do
      [] -> nil
      [table] -> table
    end
  end

  def register_cluster_info(cluster, info) do
    :ets.insert(@cluster_info, {cluster, info})
  end

  def lookup_cluster_info(cluster) do
    case :ets.lookup(@cluster_info, cluster) do
      [] -> nil
      [{^cluster, info}] -> info
    end
  end
end
