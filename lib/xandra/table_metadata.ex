defmodule Xandra.TableMetadata do
  alias __MODULE__

  defstruct [
    :cluster_name,
    :keyspace_name,
    :table_name,
    :partition_keys,
    :clustering_keys,
    :columns
  ]

  def from_columns(
        cluster_name,
        [%{keyspace_name: keyspace_name, table_name: table_name} | _] = columns
      ) do
    {partition_keys, columns} = columns |> Enum.split_with(&(&1[:kind] == "partition_key"))

    partition_keys =
      partition_keys
      |> Enum.sort_by(& &1[:position])
      |> Enum.map(&{&1[:type], &1[:column_name]})

    {clustering_keys, columns} = columns |> Enum.split_with(&(&1[:kind] == "clustering"))

    clustering_keys =
      clustering_keys
      |> Enum.sort_by(& &1[:position])
      |> Enum.map(&{&1[:type], &1[:column_name], &1[:clustering_order] || "ASC"})

    columns =
      columns
      |> Enum.map(&{&1[:type], &1[:column_name]})

    %TableMetadata{
      cluster_name: cluster_name,
      keyspace_name: keyspace_name,
      table_name: table_name,
      partition_keys: partition_keys,
      clustering_keys: clustering_keys,
      columns: columns
    }
  end
end
