package scuttlebutt

import (
	"bytes"
	"encoding/binary"
)

type digest struct {
	NodeMaxVersion map[Node]version
}

func newDigest() *digest {
	return &digest{NodeMaxVersion: make(map[Node]version)}
}

func (d *digest) addNode(node Node, maxVersion version) {
	d.NodeMaxVersion[node] = maxVersion
}

func (d *digest) getNodeMaxVersion(node Node) (version, bool) {
	if v, ok := d.NodeMaxVersion[node]; ok {
		return v, ok
	}
	return 0, false
}

func (d *digest) serialize() ([]byte, error) {
	var b bytes.Buffer
	b.Grow(d.serializedSize())

	err := binary.Write(&b, binary.BigEndian, uint16(len(d.NodeMaxVersion)))
	if err != nil {
		return nil, err
	}

	for k, v := range d.NodeMaxVersion {
		if err := k.serializeTo(&b); err != nil {
			return nil, err
		}
		err = binary.Write(&b, binary.BigEndian, uint64(v))
		if err != nil {
			return nil, err
		}
	}
	return b.Bytes(), nil
}

func (d *digest) serializedSize() int {
	bytes := 2
	for k := range d.NodeMaxVersion {
		bytes += k.serializedSize() + 8
	}
	return bytes
}

func (d *digest) deserialize(buf []byte) error {
	b := bytes.NewBuffer(buf)

	var elem uint16
	if err := binary.Read(b, binary.BigEndian, &elem); err != nil {
		return err
	}

	d.NodeMaxVersion = make(map[Node]version, elem)
	for i := 0; i < int(elem); i++ {
		node := Node{}
		if err := node.deserializeFrom(b); err != nil {
			return err
		}

		var ver uint64
		if err := binary.Read(b, binary.BigEndian, &ver); err != nil {
			return err
		}

		d.NodeMaxVersion[node] = version(ver)
	}
	return nil
}

type digestWriter struct {
	digest *digest
	mtu    int
	full   bool
	bytes  int
	node   Node
}

func newDigestWriter(mtu int) *digestWriter {
	return &digestWriter{digest: newDigest(), mtu: mtu, full: false, bytes: 2}
}

func (dw *digestWriter) addBytes(n int) bool {
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

func (dw *digestWriter) addNode(node Node, maxVersion version) bool {
	if node == dw.node {
		panic("Node already exists")
	}

	if !dw.addBytes(node.serializedSize() + 8) {
		return false
	}

	dw.node = node
	dw.digest.addNode(node, maxVersion)
	return true
}

func (dw *digestWriter) getDigest() *digest {
	return dw.digest
}
