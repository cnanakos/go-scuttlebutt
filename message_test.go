package scuttlebutt

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func getTestUUID() string {
	return uuid.New().String()
}

func getTestNode() Node {
	return Node{ID: getTestUUID(), GossipPublicAddress: testListenAddr}
}

func TestSynMessage(t *testing.T) {
	clusterID := getTestUUID()
	node := getTestNode()
	digest := newDigest()
	maxVersion := version(200)
	digest.addNode(node, maxVersion)

	m := synMessage{ClusterID: clusterID, Digest: digest}

	buf, err := m.serialize()
	require.Nil(t, err)

	mm := synMessage{}
	err = mm.deserialize(buf)
	require.Nil(t, err)

	require.Equal(t, mm.ClusterID, clusterID)
	require.Equal(t, mm.ClusterID, m.ClusterID)

	mVersion, ok := mm.Digest.getNodeMaxVersion(node)
	require.Equal(t, ok, true)
	require.Equal(t, mVersion, maxVersion)
}

func TestAckMessage(t *testing.T) {
	node := getTestNode()

	digest := newDigest()
	maxVersion := version(200)
	digest.addNode(node, maxVersion)

	delta := newDelta()
	key := "test_key"
	value := "test_value"
	valueVersion := version(888)
	delta.addNodeDeltaKV(node, key, value, valueVersion)

	m := ackMessage{Digest: digest, Delta: delta}

	buf, err := m.serialize()
	require.Nil(t, err)

	mm := ackMessage{}
	err = mm.deserialize(buf)
	require.Nil(t, err)

	nd := mm.Delta.getNodeDelta(node)
	require.NotNil(t, nd)
	require.Equal(t, nd.getKey(key).Value, value)
	require.Equal(t, nd.getKey(key).Version, valueVersion)

	mVersion, ok := mm.Digest.getNodeMaxVersion(node)
	require.Equal(t, ok, true)
	require.Equal(t, mVersion, maxVersion)
}

func TestAck2Message(t *testing.T) {
	node := getTestNode()

	delta := newDelta()
	key := "test_key"
	value := "test_value"
	valueVersion := version(888)
	delta.addNodeDeltaKV(node, key, value, valueVersion)

	m := ack2Message{Delta: delta}

	buf, err := m.serialize()
	require.Nil(t, err)

	mm := ack2Message{}
	err = mm.deserialize(buf)
	require.Nil(t, err)

	nd := mm.Delta.getNodeDelta(node)
	require.NotNil(t, nd)
	require.Equal(t, nd.getKey(key).Value, value)
	require.Equal(t, nd.getKey(key).Version, valueVersion)
}

func TestWrongSynPacket(t *testing.T) {
	pktBuf := []byte{uint8(synMsg)}
	_, _, err := deserializePacket(pktBuf)
	require.NotNil(t, err)

	pktBuf = []byte{uint8(synMsg), 1, 2, 3, 4, 5}
	_, _, err = deserializePacket(pktBuf)
	require.NotNil(t, err)
}

func TestWrongAckPacket(t *testing.T) {
	pktBuf := []byte{uint8(ackMsg)}
	_, _, err := deserializePacket(pktBuf)
	require.NotNil(t, err)

	pktBuf = []byte{uint8(ackMsg), 1, 2, 3}
	_, _, err = deserializePacket(pktBuf)
	require.NotNil(t, err)

	pktBuf = []byte{uint8(ackMsg), 0x1, 0x0, 0x0, 0x0}
	_, _, err = deserializePacket(pktBuf)
	require.NotNil(t, err)

	pktBuf = []byte{uint8(ackMsg), 0x1, 0x0, 0x0, 0x0, 'W'}
	_, _, err = deserializePacket(pktBuf)
	require.NotNil(t, err)

	pktBuf = []byte{uint8(ackMsg), 0x0, 0x0, 0x0, 0xf, 'W'}
	_, _, err = deserializePacket(pktBuf)
	require.NotNil(t, err)

	pktBuf = []byte{uint8(ackMsg), 0xf, 0xf, 0xf, 0xf, 'W'}
	_, _, err = deserializePacket(pktBuf)
	require.NotNil(t, err)
}

func TestWrongAck2Packet(t *testing.T) {
	pktBuf := []byte{uint8(ack2Msg)}
	_, _, err := deserializePacket(pktBuf)
	require.NotNil(t, err)

	pktBuf = []byte{uint8(ack2Msg), 1, 2, 3, 4, 5}
	_, _, err = deserializePacket(pktBuf)
	require.NotNil(t, err)
}

func TestWrongEncryptedPacket(t *testing.T) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.Nil(t, err)

	pktBuf := []byte{uint8(encryptedMsg)}
	_, err = decryptSerializedPacket(key, pktBuf[1:])
	require.NotNil(t, err)

	pktBuf = []byte{uint8(encryptedMsg), 1, 2, 3, 4, 5}
	_, err = decryptSerializedPacket(key, pktBuf[1:])
	require.NotNil(t, err)
}

func TestPackets(t *testing.T) {
	clusterID := getTestUUID()
	node := getTestNode()

	digest := newDigest()
	maxVersion := version(200)
	digest.addNode(node, maxVersion)

	pktBuf, err := serializeSynPacket(clusterID, digest)
	require.Nil(t, err)

	msgType, pkt, err := deserializePacket(pktBuf)
	require.Equal(t, synMsg, msgType)
	require.Nil(t, err)
	_, ok := pkt.(synMessage)
	require.Equal(t, true, ok)
	_, ok = pkt.(ackMessage)
	require.Equal(t, false, ok)
	_, ok = pkt.(ack2Message)
	require.Equal(t, false, ok)
	_, ok = pkt.(wrongClusterMessage)
	require.Equal(t, false, ok)

	synMsg := pkt.(synMessage)
	require.Equal(t, synMsg.ClusterID, clusterID)
	require.Equal(t, synMsg.Digest, digest)

	delta := newDelta()
	key := "test_key"
	value := "test_value"
	valueVersion := version(888)
	delta.addNodeDeltaKV(node, key, value, valueVersion)

	pktBuf, err = serializeAckPacket(digest, delta)
	require.Nil(t, err)
	msgType, pkt, err = deserializePacket(pktBuf)
	require.Equal(t, messageType(ackMsg), msgType)
	require.Nil(t, err)
	_, ok = pkt.(synMessage)
	require.Equal(t, false, ok)
	_, ok = pkt.(ackMessage)
	require.Equal(t, true, ok)
	_, ok = pkt.(ack2Message)
	require.Equal(t, false, ok)
	_, ok = pkt.(wrongClusterMessage)
	require.Equal(t, false, ok)

	ackMsg := pkt.(ackMessage)
	require.Equal(t, ackMsg.Delta, delta)
	require.Equal(t, ackMsg.Digest, digest)

	pktBuf, err = serializeAck2Packet(delta)
	require.Nil(t, err)
	msgType, pkt, err = deserializePacket(pktBuf)
	require.Equal(t, messageType(ack2Msg), msgType)
	require.Nil(t, err)
	_, ok = pkt.(synMessage)
	require.Equal(t, false, ok)
	_, ok = pkt.(ackMessage)
	require.Equal(t, false, ok)
	_, ok = pkt.(ack2Message)
	require.Equal(t, true, ok)
	_, ok = pkt.(wrongClusterMessage)
	require.Equal(t, false, ok)

	ack2Msg := pkt.(ack2Message)
	require.Equal(t, ack2Msg.Delta, delta)
}

func TestPacketsCompressed(t *testing.T) {
	clusterID := getTestUUID()
	node := getTestNode()

	digest := newDigest()
	maxVersion := version(200)
	digest.addNode(node, maxVersion)

	pktBuf, err := serializeSynPacket(clusterID, digest)
	require.Nil(t, err)
	pktBuf, err = compressSerializedPacket(pktBuf)
	require.Nil(t, err)

	msgType, pkt, err := deserializePacket(pktBuf)
	require.Equal(t, synMsg, msgType)
	require.Nil(t, err)
	_, ok := pkt.(synMessage)
	require.Equal(t, true, ok)
	_, ok = pkt.(ackMessage)
	require.Equal(t, false, ok)
	_, ok = pkt.(ack2Message)
	require.Equal(t, false, ok)
	_, ok = pkt.(wrongClusterMessage)
	require.Equal(t, false, ok)

	synMsg := pkt.(synMessage)
	require.Equal(t, synMsg.ClusterID, clusterID)
	require.Equal(t, synMsg.Digest, digest)

	delta := newDelta()
	key := "test_key"
	value := "test_value"
	valueVersion := version(888)
	delta.addNodeDeltaKV(node, key, value, valueVersion)

	pktBuf, err = serializeAckPacket(digest, delta)
	require.Nil(t, err)
	pktBuf, err = compressSerializedPacket(pktBuf)
	require.Nil(t, err)

	msgType, pkt, err = deserializePacket(pktBuf)
	require.Equal(t, messageType(ackMsg), msgType)
	require.Nil(t, err)
	_, ok = pkt.(synMessage)
	require.Equal(t, false, ok)
	_, ok = pkt.(ackMessage)
	require.Equal(t, true, ok)
	_, ok = pkt.(ack2Message)
	require.Equal(t, false, ok)
	_, ok = pkt.(wrongClusterMessage)
	require.Equal(t, false, ok)

	ackMsg := pkt.(ackMessage)
	require.Equal(t, ackMsg.Delta, delta)
	require.Equal(t, ackMsg.Digest, digest)

	pktBuf, err = serializeAck2Packet(delta)
	require.Nil(t, err)
	pktBuf, err = compressSerializedPacket(pktBuf)
	require.Nil(t, err)

	msgType, pkt, err = deserializePacket(pktBuf)
	require.Equal(t, messageType(ack2Msg), msgType)
	require.Nil(t, err)
	_, ok = pkt.(synMessage)
	require.Equal(t, false, ok)
	_, ok = pkt.(ackMessage)
	require.Equal(t, false, ok)
	_, ok = pkt.(ack2Message)
	require.Equal(t, true, ok)
	_, ok = pkt.(wrongClusterMessage)
	require.Equal(t, false, ok)

	ack2Msg := pkt.(ack2Message)
	require.Equal(t, ack2Msg.Delta, delta)
}
