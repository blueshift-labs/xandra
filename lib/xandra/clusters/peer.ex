defmodule Xandra.Clusters.Peer do
  @enforce_keys [
    :cluster_name,
    :host_id,
    :rpc_address,
    :port,
    :data_center,
    :rack,
    :cql_version,
    :tokens
  ]
  defstruct [
    :cluster_name,
    :host_id,
    :rpc_address,
    :port,
    :data_center,
    :rack,
    :cql_version,
    :tokens
  ]

  def from_system_local(cluster_name, port, %{
        host_id: host_id,
        rpc_address: rpc_address,
        data_center: data_center,
        rack: rack,
        cql_version: cql_version,
        tokens: tokens
      }) do
    %__MODULE__{
      cluster_name: cluster_name,
      host_id: host_id,
      rpc_address: rpc_address,
      port: port,
      data_center: data_center,
      rack: rack,
      cql_version: cql_version,
      tokens: tokens
    }
  end
end
