defmodule Xandra.Cluster.Host do
  @enforce_keys [:host_id]
  defstruct [
    :host_id,
    :broadcast_address,
    :listen_address,
    :rpc_address,
    :data_center,
    :partitioner,
    :rack,
    tokens: MapSet.new([])
  ]

  def from_map(%{
        "host_id" => host_id,
        "broadcast_address" => broadcast_address,
        "listen_address" => listen_address,
        "rpc_address" => rpc_address,
        "data_center" => data_center,
        "partitioner" => partitioner,
        "rack" => rack,
        "tokens" => tokens
      }) do
    %__MODULE__{
      host_id: host_id,
      broadcast_address: broadcast_address,
      listen_address: listen_address,
      rpc_address: rpc_address,
      data_center: data_center,
      partitioner: partitioner,
      rack: rack,
      tokens: tokens
    }
  end
end
