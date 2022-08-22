package scuttlebutt

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func (node *Node) serializeTo(b *bytes.Buffer) error {
	err := binary.Write(b, binary.BigEndian, uint16(len(node.ID)))
	if err != nil {
		return err
	}

	n, err := b.Write([]byte(node.ID))
	if err != nil {
		return err
	}

	if n != len(node.ID) {
		return fmt.Errorf(errShortWrite, len(node.ID), n)
	}

	err = binary.Write(b, binary.BigEndian, uint16(len(node.GossipPublicAddress)))
	if err != nil {
		return err
	}

	n, err = b.Write([]byte(node.GossipPublicAddress))
	if err != nil {
		return err
	}

	if n != len(node.GossipPublicAddress) {
		return fmt.Errorf(errShortWrite, len(node.GossipPublicAddress), n)
	}

	return nil
}

func (node *Node) deserializeFrom(b *bytes.Buffer) error {
	var Len uint16
	if err := binary.Read(b, binary.BigEndian, &Len); err != nil {
		return err
	}

	id := make([]byte, Len)
	n, err := b.Read(id)
	if err != nil {
		return err
	}

	if n != int(Len) {
		return fmt.Errorf(errUnexpectedBufferLen, Len, n)
	}

	if err := binary.Read(b, binary.BigEndian, &Len); err != nil {
		return err
	}

	p := make([]byte, Len)
	n, err = b.Read(p)
	if err != nil {
		return err
	}

	if n != int(Len) {
		return fmt.Errorf(errUnexpectedBufferLen, Len, n)
	}

	node.ID = string(id)
	node.GossipPublicAddress = string(p)
	return nil
}

func (node *Node) serializedSize() int {
	return (2 + len(node.ID) + 2 + len(node.GossipPublicAddress))
}

func (v *versionedValue) serializeTo(b *bytes.Buffer) error {
	err := binary.Write(b, binary.BigEndian, uint16(len(v.Value)))
	if err != nil {
		return err
	}

	n, err := b.Write([]byte(v.Value))
	if err != nil {
		return err
	}

	if n != len(v.Value) {
		return fmt.Errorf(errShortWrite, len(v.Value), n)
	}

	err = binary.Write(b, binary.BigEndian, uint64(v.Version))
	if err != nil {
		return err
	}

	return nil
}

func (v *versionedValue) deserializeFrom(b *bytes.Buffer) error {
	var valueLen uint16
	if err := binary.Read(b, binary.BigEndian, &valueLen); err != nil {
		return err
	}

	value := make([]byte, valueLen)
	n, err := b.Read(value)
	if err != nil {
		return err
	}

	if n != int(valueLen) {
		return fmt.Errorf(errUnexpectedBufferLen, valueLen, n)
	}

	var ver uint64
	if err := binary.Read(b, binary.BigEndian, &ver); err != nil {
		return err
	}

	v.Value = string(value)
	v.Version = version(ver)
	return nil
}

func (v *versionedValue) serializedSize() int {
	return (2 + len(v.Value) + 8)
}
