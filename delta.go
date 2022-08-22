package scuttlebutt

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type version uint64

type versionedValue struct {
	Value   string
	Version version
}

type nodeDelta struct {
	KV map[string]versionedValue
}

func newNodeDelta() *nodeDelta {
	return &nodeDelta{KV: make(map[string]versionedValue)}
}

func (nd *nodeDelta) addKeyVersioned(key, value string, valueVersion version) {
	nd.KV[key] = versionedValue{Value: value, Version: valueVersion}
}

func (nd *nodeDelta) removeKey(key string) {
	if _, ok := nd.KV[key]; ok {
		delete(nd.KV, key)
	}
}

func (nd *nodeDelta) getKey(key string) versionedValue {
	return nd.KV[key]
}

func (nd *nodeDelta) maxVersion() version {
	var max version
	for _, v := range nd.KV {
		if v.Version > max {
			max = v.Version
		}
	}
	return max
}

type delta struct {
	NodeDeltas map[Node]*nodeDelta
}

func newDelta() *delta {
	return &delta{NodeDeltas: make(map[Node]*nodeDelta)}
}

func (d *delta) addNodeDeltaKV(node Node, key, value string, valueVersion version) {
	if _, ok := d.NodeDeltas[node]; !ok {
		d.NodeDeltas[node] = newNodeDelta()
	}
	d.NodeDeltas[node].addKeyVersioned(key, value, valueVersion)
}

func (d *delta) addNodeDelta(node Node, nd *nodeDelta) {
	d.NodeDeltas[node] = nd
}

func (d *delta) getNodeDelta(node Node) *nodeDelta {
	if n, ok := d.NodeDeltas[node]; ok {
		return n
	}
	return nil
}

func (d *delta) removeNode(node Node) {
	if _, ok := d.NodeDeltas[node]; ok {
		delete(d.NodeDeltas, node)
	}
}

func (d *delta) serializedSize() int {
	bytes := 2
	for k, v := range d.NodeDeltas {
		bytes += k.serializedSize() + 2
		for kk, vv := range v.KV {
			bytes += 2 + len(kk) + vv.serializedSize()
		}
	}
	return bytes
}

func (d *delta) serialize() ([]byte, error) {
	var b bytes.Buffer
	b.Grow(d.serializedSize())

	err := binary.Write(&b, binary.BigEndian, uint16(len(d.NodeDeltas)))
	if err != nil {
		return nil, err
	}

	for k, v := range d.NodeDeltas {
		if err := k.serializeTo(&b); err != nil {
			return nil, err
		}

		err = binary.Write(&b, binary.BigEndian, uint16(len(v.KV)))
		if err != nil {
			return nil, err
		}

		for kk, vv := range v.KV {
			err := binary.Write(&b, binary.BigEndian, uint16(len(kk)))
			if err != nil {
				return nil, err
			}

			if n, err := b.Write([]byte(kk)); err != nil {
				return nil, err
			} else if n != len(kk) {
				return nil, fmt.Errorf(errShortWrite, len(kk), n)
			}

			if err := vv.serializeTo(&b); err != nil {
				return nil, err
			}
		}
	}
	return b.Bytes(), nil
}

func (d *delta) deserialize(buf []byte) error {
	b := bytes.NewBuffer(buf)

	var elements uint16
	if err := binary.Read(b, binary.BigEndian, &elements); err != nil {
		return err
	}
	d.NodeDeltas = make(map[Node]*nodeDelta, elements)

	for i := 0; i < int(elements); i++ {
		node := Node{}
		if err := node.deserializeFrom(b); err != nil {
			return err
		}

		nd := newNodeDelta()
		d.NodeDeltas[node] = nd

		var kvElem uint16
		if err := binary.Read(b, binary.BigEndian, &kvElem); err != nil {
			return err
		}

		for j := 0; j < int(kvElem); j++ {
			var keyLen uint16
			if err := binary.Read(b, binary.BigEndian, &keyLen); err != nil {
				return err
			}

			key := make([]byte, keyLen)
			if n, err := b.Read(key); err != nil {
				return err
			} else if n != int(keyLen) {
				return fmt.Errorf(errUnexpectedBufferLen, keyLen, n)
			}

			vv := versionedValue{}
			if err := vv.deserializeFrom(b); err != nil {
				return err
			}

			nd.KV[string(key)] = vv
		}
	}
	return nil
}

type deltaWriter struct {
	delta *delta
	mtu   int
	full  bool
	bytes int

	node      Node
	nodeDelta *nodeDelta
}

func newDeltaWriter(mtu int) *deltaWriter {
	return &deltaWriter{delta: newDelta(), mtu: mtu, full: false, bytes: 4}
}

func (dw *deltaWriter) addBytes(n int) bool {
	if dw.full {
		return false
	}

	bytes := dw.bytes + n
	if bytes > dw.mtu {
		dw.full = true
		return false
	}
	dw.bytes = bytes
	return true
}

func (dw *deltaWriter) addNode(node Node) bool {
	if node == dw.node || dw.delta.getNodeDelta(node) != nil {
		panic("Node already exists")
	}

	if !dw.addBytes(2 + len(node.ID) + 2 + len(node.GossipPublicAddress) + 2) {
		return false
	}

	dw.node = node
	dw.nodeDelta = newNodeDelta()
	dw.delta.addNodeDelta(dw.node, dw.nodeDelta)
	return true
}

func (dw *deltaWriter) addKV(key string, value versionedValue) bool {
	if !dw.addBytes(2 + len(key) + 2 + len(value.Value) + 8) {
		return false
	}
	dw.nodeDelta.addKeyVersioned(key, value.Value, value.Version)
	return true
}

func (dw *deltaWriter) getDelta() *delta {
	return dw.delta
}
