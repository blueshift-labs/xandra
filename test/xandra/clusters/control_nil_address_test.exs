defmodule Xandra.Clusters.ControlNilAddressTest do
  use ExUnit.Case, async: true

  alias Xandra.Clusters.Control

  describe "valid_peer_address?/1" do
    test "returns false for nil rpc_address" do
      peer = %{host_id: "test-id", rpc_address: nil, data_center: "dc1"}
      refute Control.valid_peer_address?(peer)
    end

    test "returns false for empty string rpc_address" do
      peer = %{host_id: "test-id", rpc_address: "", data_center: "dc1"}
      refute Control.valid_peer_address?(peer)
    end

    test "returns false for 0.0.0.0 rpc_address" do
      peer = %{host_id: "test-id", rpc_address: "0.0.0.0", data_center: "dc1"}
      refute Control.valid_peer_address?(peer)
    end

    test "returns true for valid rpc_address" do
      peer = %{host_id: "test-id", rpc_address: "192.168.1.100", data_center: "dc1"}
      assert Control.valid_peer_address?(peer)
    end

    test "returns true for valid IP tuple rpc_address" do
      peer = %{host_id: "test-id", rpc_address: {192, 168, 1, 100}, data_center: "dc1"}
      assert Control.valid_peer_address?(peer)
    end
  end

  describe "peer filtering during topology changes" do
    test "filters out peers with invalid addresses" do
      system_peers = [
        %{host_id: "valid-1", rpc_address: "192.168.1.100", data_center: "dc1"},
        %{host_id: "invalid-nil", rpc_address: nil, data_center: "dc1"},
        %{host_id: "invalid-empty", rpc_address: "", data_center: "dc1"},
        %{host_id: "invalid-zero", rpc_address: "0.0.0.0", data_center: "dc1"},
        %{host_id: "valid-2", rpc_address: "192.168.1.101", data_center: "dc1"}
      ]

      cluster_status = %{
        "valid-1" => true,
        "invalid-nil" => true,
        "invalid-empty" => true,
        "invalid-zero" => true,
        "valid-2" => true
      }

      data_center = "dc1"

      valid_peers =
        system_peers
        |> Enum.filter(&match?(%{data_center: ^data_center}, &1))
        |> Enum.filter(&cluster_status[&1[:host_id]])
        |> Enum.filter(&Control.valid_peer_address?/1)

      assert length(valid_peers) == 2
      assert Enum.all?(valid_peers, fn peer -> peer.host_id in ["valid-1", "valid-2"] end)
    end
  end
end
