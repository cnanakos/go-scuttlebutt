package scuttlebutt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDigest(t *testing.T) {
	node := Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}
	d := newDigest()

	maxVersion := version(200)
	d.addNode(node, maxVersion)

	buf, err := d.serialize()
	require.Nil(t, err)

	dd := newDigest()
	err = dd.deserialize(buf)
	require.Nil(t, err)

	mv, ok := dd.getNodeMaxVersion(node)
	require.Equal(t, ok, true)
	require.Equal(t, mv, maxVersion)

	wrongNode := Node{ID: "wrong", GossipPublicAddress: testListenAddr}
	_, ok = dd.getNodeMaxVersion(wrongNode)
	require.Equal(t, ok, false)
}
