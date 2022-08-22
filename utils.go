package scuttlebutt

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"sort"

	mrand "math/rand"
)

const (
	aesNonceSize = 12
)

type reverseSortedNodes map[int][]Node

func (sn reverseSortedNodes) sort() (index []int) {
	for k := range sn {
		index = append(index, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(index)))
	return
}

func setUDPRecvBuf(c *net.UDPConn) error {
	size := udpRecvBufSize
	var err error
	for size > 0 {
		if err = c.SetReadBuffer(size); err == nil {
			return nil
		}
		size = size / 2
	}
	return err
}

func compressBuffer(buf []byte) ([]byte, error) {
	var b bytes.Buffer
	gw := gzip.NewWriter(&b)

	_, err := gw.Write(buf)
	if err != nil {
		return nil, err
	}

	if err := gw.Flush(); err != nil {
		return nil, err
	}

	if err := gw.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func decompressBuffer(buf []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	var b bytes.Buffer
	_, err = io.Copy(&b, gr)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func encryptBuffer(key []byte, plaintext []byte) ([]byte, error) {
	aesBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	aesGCM, err := cipher.NewGCM(aesBlock)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, aesNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	cipherText := aesGCM.Seal(nonce, nonce, plaintext, nil)
	return cipherText, nil
}

func decryptBuffer(key []byte, buf []byte) ([]byte, error) {
	if len(buf) < aesNonceSize+1 {
		return nil, fmt.Errorf(errUnexpectedBufferLen, aesNonceSize+1, len(buf))
	}

	aesBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	aesGCM, err := cipher.NewGCM(aesBlock)
	if err != nil {
		return nil, err
	}

	nonce, cipherText := buf[:aesNonceSize], buf[aesNonceSize:]

	plaintext, err := aesGCM.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

func diffNodeLists(nodesA, nodesB []Node) (extraA, extraB []Node) {
	aLen := len(nodesA)
	bLen := len(nodesB)

	visited := make([]bool, bLen)
	for i := 0; i < aLen; i++ {
		element := nodesA[i]
		found := false
		for j := 0; j < bLen; j++ {
			if visited[j] {
				continue
			}
			if element == nodesB[j] {
				visited[j] = true
				found = true
				break
			}
		}
		if !found {
			extraA = append(extraA, element)
		}
	}

	for j := 0; j < bLen; j++ {
		if visited[j] {
			continue
		}

		extraB = append(extraB, nodesB[j])
	}
	return
}

func nodesMatch(nodesA, nodesB []Node) bool {
	if len(nodesA) == 0 && len(nodesB) == 0 {
		return true
	}

	if len(nodesA) != len(nodesB) {
		return false
	}

	a, b := diffNodeLists(nodesA, nodesB)

	if len(a) == 0 && len(b) == 0 {
		return true
	}

	return false
}

func getRandomNodes(k int, nodes []Node, exclude Node) []Node {
	if len(nodes) <= k {
		return nodes
	}

	randNodes := make([]Node, 0, k)

LOOP:
	for {
		idx := int(mrand.Uint32() % uint32(len(nodes)))
		node := nodes[idx]

		if node == exclude {
			continue
		}

		for j := 0; j < len(randNodes); j++ {
			if randNodes[j] == node {
				continue LOOP
			}
		}

		randNodes = append(randNodes, node)
		if len(randNodes) == k {
			break
		}
	}

	return randNodes
}
