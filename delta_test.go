package scuttlebutt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDelta(t *testing.T) {
	node := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	d := newDelta()
	key := "test_key"
	value := "test_value"
	valueVersion := version(888)
	d.addNodeDeltaKV(node, key, value, valueVersion)

	buf, err := d.serialize()
	require.Nil(t, err)

	dd := newDelta()
	err = dd.deserialize(buf)
	require.Nil(t, err)

	nd := dd.getNodeDelta(node)
	require.NotNil(t, nd)
	require.Equal(t, nd.getKey(key).Value, value)
	require.Equal(t, nd.getKey(key).Version, valueVersion)

	wrongNode := Node{ID: "wrong", GossipPublicAddress: testListenAddr}
	wnd := dd.getNodeDelta(wrongNode)
	require.Nil(t, wnd)
}

func TestDeltaWriterSimple(t *testing.T) {
	node1 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}
	node2 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	dw := newDeltaWriter(311)
	require.Equal(t, dw.addNode(node1), true)
	require.Equal(t, dw.addKV("key11", versionedValue{"val11", 1}), true)
	require.Equal(t, dw.addKV("key12", versionedValue{"val12", 2}), true)

	dw.addNode(node2)
	dw.addKV("key21", versionedValue{"val21", 2})
	dw.addKV("key22", versionedValue{"val22", 3})

	delta := dw.getDelta()
	buf, err := delta.serialize()
	require.Nil(t, err)
	require.Equal(t, len(buf), delta.serializedSize())
}

func TestDeltaWriterSingleNode(t *testing.T) {
	node1 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}
	node2 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	dw := newDeltaWriter(132)
	dw.addNode(node1)
	dw.addKV("key11", versionedValue{"val11", 1})
	dw.addKV("key12", versionedValue{"val12", 2})

	require.Equal(t, false, dw.addNode(node2))

	delta := dw.getDelta()
	buf, err := delta.serialize()
	require.Nil(t, err)
	require.Equal(t, len(buf), delta.serializedSize())
}

func TestDeltaWriterNoKeySecondNode(t *testing.T) {
	node1 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}
	node2 := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}

	dw := newDeltaWriter(162)
	dw.addNode(node1)
	dw.addKV("key11", versionedValue{"val11", 1})
	dw.addKV("key12", versionedValue{"val12", 2})

	require.Equal(t, true, dw.addNode(node2))
	require.Equal(t, false, dw.addKV("key21", versionedValue{"val21", 1}))

	delta := dw.getDelta()
	buf, err := delta.serialize()
	require.Nil(t, err)
	require.Equal(t, len(buf), delta.serializedSize())
}
