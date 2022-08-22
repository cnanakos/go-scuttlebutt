package scuttlebutt

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestApplyDelta(t *testing.T) {
	node := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	clusterState := newClusterState()
	nodeState := clusterState.getNodeStateDefault(node)
	nodeState.setWithVersion("key1", "1", 1)
	nodeState.setWithVersion("key2", "3", 3)

	delta := newDelta()
	delta.addNodeDeltaKV(node, "key1", "4", 4)
	delta.addNodeDeltaKV(node, "key2", "2", 2) //stale value

	clusterState.applyDelta(delta)
	nodeState = clusterState.getNodeStateDefault(node)
	k1, ok := nodeState.getVersioned("key1")
	require.Equal(t, ok, true)
	require.Equal(t, k1.Value, "4")
	require.Equal(t, k1.Version, version(4))

	k2, ok := nodeState.getVersioned("key2")
	require.Equal(t, ok, true)
	require.Equal(t, k2.Value, "3")
	require.Equal(t, k2.Version, version(3))
}

func TestComputeDigest(t *testing.T) {
	node1 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}
	node2 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	clusterState := newClusterState()
	nodestate1 := clusterState.getNodeStateDefault(node1)
	nodestate1.set("key1", "")
	nodestate1.set("key2", "")

	nodestate2 := clusterState.getNodeStateDefault(node2)
	nodestate2.set("key1", "")

	digest := clusterState.computeDigest(nil)
	nodeMaxVersion := make(map[Node]version)
	nodeMaxVersion[node1] = 2
	nodeMaxVersion[node2] = 1
	require.Equal(t, digest.NodeMaxVersion, nodeMaxVersion)

	deadNodes := []Node{node1}
	digest = clusterState.computeDigest(deadNodes)
	nodeMaxVersion = make(map[Node]version)
	nodeMaxVersion[node2] = 1
	require.Equal(t, digest.NodeMaxVersion, nodeMaxVersion)
}

func TestComputeDeltaSimple(t *testing.T) {
	node1 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}
	node2 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	clusterState := newClusterState()
	nodestate1 := clusterState.getNodeStateDefault(node1)
	nodestate1.setWithVersion("key1", "1", 1)
	nodestate1.setWithVersion("key2", "2", 2)

	nodestate2 := clusterState.getNodeStateDefault(node2)
	nodestate2.setWithVersion("key1", "1", 1)
	nodestate2.setWithVersion("key2", "2", 2)
	nodestate2.setWithVersion("key3", "3", 3)
	nodestate2.setWithVersion("key4", "4", 4)

	digest := newDigest()
	digest.addNode(node1, 1)
	digest.addNode(node2, 2)

	delta := clusterState.computeDelta(1024, digest, nil)
	buf, err := delta.serialize()
	require.Nil(t, err)
	require.LessOrEqual(t, len(buf), 1024)

	require.Equal(t, map[string]versionedValue{"key2": versionedValue{"2", 2}},
		delta.NodeDeltas[node1].KV)
	require.Equal(t, map[string]versionedValue{"key3": versionedValue{"3", 3},
		"key4": versionedValue{"4", 4}},
		delta.NodeDeltas[node2].KV)
}

func TestComputeDeltaMissingNode(t *testing.T) {
	node1 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}
	node2 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	clusterState := newClusterState()
	nodestate1 := clusterState.getNodeStateDefault(node1)
	nodestate1.setWithVersion("key1", "1", 1)
	nodestate1.setWithVersion("key2", "2", 2)

	nodestate2 := clusterState.getNodeStateDefault(node2)
	nodestate2.setWithVersion("key1", "1", 1)
	nodestate2.setWithVersion("key2", "2", 2)
	nodestate2.setWithVersion("key3", "3", 3)
	nodestate2.setWithVersion("key4", "4", 4)

	digest := newDigest()
	digest.addNode(node2, 3)

	delta := clusterState.computeDelta(1024, digest, nil)

	require.Equal(t, map[string]versionedValue{"key1": versionedValue{"1", 1},
		"key2": versionedValue{"2", 2}},
		delta.NodeDeltas[node1].KV)
	require.Equal(t, map[string]versionedValue{"key4": versionedValue{"4", 4}},
		delta.NodeDeltas[node2].KV)
}

func TestComputeDeltaIgnoreDeadNodes(t *testing.T) {
	node1 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}
	node2 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	clusterState := newClusterState()
	nodestate1 := clusterState.getNodeStateDefault(node1)
	nodestate1.setWithVersion("key1", "1", 1)
	nodestate1.setWithVersion("key2", "2", 2)

	nodestate2 := clusterState.getNodeStateDefault(node2)
	nodestate2.setWithVersion("key1", "1", 1)
	nodestate2.setWithVersion("key2", "2", 2)
	nodestate2.setWithVersion("key3", "3", 3)
	nodestate2.setWithVersion("key4", "4", 4)

	digest := newDigest()
	delta := clusterState.computeDelta(1024, digest, []Node{node2})

	require.Equal(t, map[string]versionedValue{"key1": versionedValue{"1", 1},
		"key2": versionedValue{"2", 2}},
		delta.NodeDeltas[node1].KV)
}

func TestClusterState(t *testing.T) {
	node := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	clusterState := newClusterState()
	nodeState := clusterState.getNodeStateDefault(node)
	nodeState.set("key1", "1")
	v, ok := nodeState.getVersioned("key1")
	require.Equal(t, ok, true)
	require.Equal(t, *v, versionedValue{"1", 1})

	nodeState.set("key2", "2")
	v, ok = nodeState.getVersioned("key1")
	require.Equal(t, ok, true)
	require.Equal(t, *v, versionedValue{"1", 1})
	v, ok = nodeState.getVersioned("key2")
	require.Equal(t, ok, true)
	require.Equal(t, *v, versionedValue{"2", 2})

	nodeState.set("key1", "3")
	v, ok = nodeState.getVersioned("key1")
	require.Equal(t, ok, true)
	require.Equal(t, *v, versionedValue{"3", 3})
}

func TestClusterStateFirstVersionIsOne(t *testing.T) {
	node := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	clusterState := newClusterState()
	nodeState := clusterState.getNodeStateDefault(node)
	nodeState.set("key1", "")
	v, ok := nodeState.getVersioned("key1")
	require.Equal(t, ok, true)
	require.Equal(t, *v, versionedValue{"", 1})
}

func TestClusterStateMissingNode(t *testing.T) {
	node := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	clusterState := newClusterState()
	nodeState, ok := clusterState.getNodeState(node)
	require.Nil(t, nodeState)
	require.Equal(t, false, ok)
}

func TestClusterStateRandomDigestConvergence(t *testing.T) {
	clusterState := newClusterState()
	count := 10000
	for i := 0; i < count; i++ {
		node := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}
		_ = clusterState.getNodeStateDefault(node)
	}

	require.Equal(t, count, clusterState.getNodesCount())

	rounds := 0
	nodes := make(map[Node]struct{})
	for {
		digest := clusterState.computeRandomDigest(udpTxBufferSize, nil)
		for k := range digest.NodeMaxVersion {
			nodes[k] = struct{}{}
		}
		rounds++

		if count == len(nodes) {
			break
		}
	}
	require.Equal(t, count, len(nodes))
	require.Less(t, rounds, 150)
}

func TestClusterStateRandomDigestLessThanMTU(t *testing.T) {
	clusterState := newClusterState()
	count := 10000
	for i := 0; i < count; i++ {
		node := Node{ID: fmt.Sprintf("%s/%d", getTestUUID(), time.Now().UnixNano()),
			GossipPublicAddress: testListenAddr}
		_ = clusterState.getNodeStateDefault(node)
	}

	require.Equal(t, count, clusterState.getNodesCount())

	rounds := 256
	for i := 0; i < rounds; i++ {
		digest := clusterState.computeRandomDigest(udpTxBufferSize, nil)
		buf, err := digest.serialize()
		require.Nil(t, err)
		require.LessOrEqual(t, len(buf), udpTxBufferSize)
	}

	for i := 0; i < rounds; i++ {
		digest := clusterState.computeRandomDigest(udpTxBufferSize/2, nil)
		buf, err := digest.serialize()
		require.Nil(t, err)
		require.LessOrEqual(t, len(buf), udpTxBufferSize/2)
	}

	for i := 0; i < rounds; i++ {
		digest := clusterState.computeRandomDigest(udpTxBufferSize/4, nil)
		buf, err := digest.serialize()
		require.Nil(t, err)
		require.LessOrEqual(t, len(buf), udpTxBufferSize/4)
	}

	for i := 0; i < rounds; i++ {
		digest := clusterState.computeRandomDigest(udpTxBufferSize/8, nil)
		buf, err := digest.serialize()
		require.Nil(t, err)
		require.LessOrEqual(t, len(buf), udpTxBufferSize/8)
	}

	for i := 0; i < rounds; i++ {
		digest := clusterState.computeRandomDigest(udpTxBufferSize/16, nil)
		buf, err := digest.serialize()
		require.Nil(t, err)
		require.LessOrEqual(t, len(buf), udpTxBufferSize/16)
	}
}

func TestClusterStateRandomDigestAndDeltaLessThanMTU(t *testing.T) {
	clusterState := newClusterState()
	count := 10000
	for i := 0; i < count; i++ {
		node := Node{ID: fmt.Sprintf("%s/%d", getTestUUID(), time.Now().UnixNano()),
			GossipPublicAddress: testListenAddr}
		nodeState := clusterState.getNodeStateDefault(node)
		nodeState.setWithVersion("key1", "1", 1)
		nodeState.setWithVersion("key2", "3", 3)
	}

	require.Equal(t, count, clusterState.getNodesCount())

	rounds := 256
	for i := 0; i < rounds; i++ {
		digest := clusterState.computeRandomDigest(udpTxBufferSize/2, nil)
		buf, err := digest.serialize()
		require.Nil(t, err)
		require.LessOrEqual(t, len(buf), udpTxBufferSize/2)
		deltaMtu := udpTxBufferSize - digest.serializedSize() - ackHeaderSize - 1
		delta := clusterState.computeDelta(deltaMtu, digest, nil)
		buf, err = delta.serialize()
		require.Nil(t, err)
		require.LessOrEqual(t, len(buf), deltaMtu)
	}
}

func TestMaxKeyValuePair(t *testing.T) {
	clusterState := newClusterState()
	node := Node{ID: fmt.Sprintf("%s/%d", getTestUUID(), time.Now().UnixNano()),
		GossipPublicAddress: testListenAddr}
	nodeState := clusterState.getNodeStateDefault(node)
	err := nodeState.setWithVersion("key1", "1", 1)
	require.Nil(t, err)

	largeKey := make([]byte, 1025)
	rand.Read(largeKey)
	err = nodeState.setWithVersion(string(largeKey), "3", 3)
	require.NotNil(t, err)

	largeValue := make([]byte, 4097)
	rand.Read(largeKey)
	err = nodeState.setWithVersion("key2", string(largeValue), 3)
	require.NotNil(t, err)
}
