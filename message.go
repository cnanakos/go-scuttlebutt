package scuttlebutt

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var (
	errUnexpectedBufferLen   = "unexpected buffer length, expected at least %d bytes and got %d"
	errShortWrite            = "expected to write %d bytes but wrote %d instead"
	errWrongPacketLenMsgType = "wrong packet length for msg type %d"
)

type messageType uint8

const (
	synMsg          messageType = 0
	ackMsg                      = 1
	ack2Msg                     = 2
	wrongClusterMsg             = 3
	compressedMsg               = 4
	encryptedMsg                = 5

	ackHeaderSize = 2
)

type synMessage struct {
	ClusterID string
	Digest    *digest
}

type ackMessage struct {
	Digest *digest
	Delta  *delta
}

type ack2Message struct {
	Delta *delta
}

type wrongClusterMessage struct {
}

func (m *synMessage) serialize() ([]byte, error) {
	var b bytes.Buffer
	b.Grow(m.serializedSize())

	err := binary.Write(&b, binary.BigEndian, uint16(len(m.ClusterID)))
	if err != nil {
		return nil, err
	}

	n, err := b.Write([]byte(m.ClusterID))
	if err != nil {
		return nil, err
	}

	if n != len(m.ClusterID) {
		return nil, fmt.Errorf(errShortWrite, len(m.ClusterID), n)
	}

	buf, err := m.Digest.serialize()
	if err != nil {
		return nil, err
	}

	err = binary.Write(&b, binary.BigEndian, uint16(len(buf)))
	if err != nil {
		return nil, err
	}

	if _, err := b.Write(buf); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (m *synMessage) serializedSize() int {
	return (2 + len(m.ClusterID) + m.Digest.serializedSize() + 2)
}

func (m *synMessage) deserialize(buf []byte) error {
	b := bytes.NewBuffer(buf)

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
		return nil
	}

	dBuf := make([]byte, Len)
	n, err = b.Read(dBuf)
	if err != nil {
		return err
	}

	if n != int(Len) {
		return fmt.Errorf(errUnexpectedBufferLen, Len, n)
	}

	m.Digest = &digest{}
	if err := m.Digest.deserialize(dBuf); err != nil {
		return err
	}

	m.ClusterID = string(id)
	return nil
}

func (m *ackMessage) serialize() ([]byte, error) {
	buf := make([]byte, ackHeaderSize)

	d, err := m.Digest.serialize()
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint16(buf, uint16(len(d)))
	buf = append(buf, d...)

	d, err = m.Delta.serialize()
	if err != nil {
		return nil, err
	}
	buf = append(buf, d...)
	return buf, nil
}

func (m *ackMessage) deserialize(buf []byte) error {
	if len(buf) < ackHeaderSize {
		return fmt.Errorf(errUnexpectedBufferLen, ackHeaderSize, len(buf))
	}

	dgSize := binary.BigEndian.Uint16(buf)
	if len(buf) < int(ackHeaderSize+dgSize) {
		return fmt.Errorf(errUnexpectedBufferLen, ackHeaderSize+dgSize, len(buf))
	}

	m.Digest = newDigest()
	err := m.Digest.deserialize(buf[ackHeaderSize : ackHeaderSize+dgSize])
	if err != nil {
		return err
	}

	m.Delta = newDelta()
	err = m.Delta.deserialize(buf[ackHeaderSize+dgSize:])
	if err != nil {
		return err
	}
	return nil
}

func (m *ack2Message) serialize() ([]byte, error) {
	d, err := m.Delta.serialize()
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (m *ack2Message) deserialize(buf []byte) error {
	m.Delta = newDelta()
	err := m.Delta.deserialize(buf)
	if err != nil {
		return err
	}
	return nil
}

func serializeSynPacket(clusterID string, digest *digest) ([]byte, error) {
	msg := synMessage{ClusterID: clusterID, Digest: digest}
	buf, err := msg.serialize()
	if err != nil {
		return nil, err
	}
	buf = append([]byte{uint8(synMsg)}, buf...)
	return buf, nil
}

func serializeAckPacket(digest *digest, delta *delta) ([]byte, error) {
	msg := ackMessage{Digest: digest, Delta: delta}
	buf, err := msg.serialize()
	if err != nil {
		return nil, err
	}
	buf = append([]byte{ackMsg}, buf...)
	return buf, nil
}

func serializeAck2Packet(delta *delta) ([]byte, error) {
	msg := ack2Message{Delta: delta}
	buf, err := msg.serialize()
	if err != nil {
		return nil, err
	}
	buf = append([]byte{uint8(ack2Msg)}, buf...)
	return buf, nil
}

func serializeWrongClusterPacket() ([]byte, error) {
	return []byte{uint8(wrongClusterMsg)}, nil
}

func encryptSerializedPacket(key, buf []byte) ([]byte, error) {
	encBuf, err := encryptBuffer(key, buf)
	if err != nil {
		return nil, err
	}

	encBuf = append([]byte{uint8(encryptedMsg)}, encBuf...)
	return encBuf, nil
}

func decryptSerializedPacket(key, buf []byte) ([]byte, error) {
	return decryptBuffer(key, buf)
}

func compressSerializedPacket(buf []byte) ([]byte, error) {
	cmprBuf, err := compressBuffer(buf)
	if err != nil {
		return nil, err
	}

	if len(cmprBuf) > len(buf) {
		return buf, nil
	}
	cmprBuf = append([]byte{uint8(compressedMsg)}, cmprBuf...)
	return cmprBuf, nil
}

func deserializePacket(buf []byte) (messageType, interface{}, error) {
	msgType := messageType(buf[0])

	if msgType == compressedMsg {
		msg, err := decompressBuffer(buf[1:])
		if err != nil {
			return msgType, nil, err
		}

		buf = msg
		msgType = messageType(buf[0])
	}

	if len(buf) < 2 && msgType != wrongClusterMsg {
		return msgType, nil, fmt.Errorf(errWrongPacketLenMsgType, msgType)
	}

	switch msgType {
	case synMsg:
		msg := synMessage{}
		err := msg.deserialize(buf[1:])
		return msgType, msg, err

	case ackMsg:
		msg := ackMessage{}
		err := msg.deserialize(buf[1:])
		return msgType, msg, err

	case ack2Msg:
		msg := ack2Message{}
		err := msg.deserialize(buf[1:])
		return msgType, msg, err

	case wrongClusterMsg:
		return msgType, wrongClusterMessage{}, nil
	}
	return msgType, nil, fmt.Errorf("unknown message type")
}
