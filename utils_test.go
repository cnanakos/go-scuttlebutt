package scuttlebutt

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompressDecompressBuffer(t *testing.T) {
	r := make([]byte, 4096)
	rand.Read(r)
	buf, err := compressBuffer(r)
	require.Nil(t, err)

	ubuf, err := decompressBuffer(buf)
	require.Nil(t, err)
	require.Equal(t, true, reflect.DeepEqual(ubuf, r))
}

func TestEncyptDecryptBufferKey16(t *testing.T) {
	plaintext := []byte("this is a test")

	key := make([]byte, 16)
	_, err := rand.Read(key)
	require.Nil(t, err)

	cipherText, err := encryptBuffer(key, plaintext)
	require.Nil(t, err)

	text, err := decryptBuffer(key, cipherText)
	require.Nil(t, err)
	require.Equal(t, true, reflect.DeepEqual(text, plaintext))
}

func TestEncyptDecryptBufferKey24(t *testing.T) {
	plaintext := []byte("this is a test")

	key := make([]byte, 24)
	_, err := rand.Read(key)
	require.Nil(t, err)

	cipherText, err := encryptBuffer(key, plaintext)
	require.Nil(t, err)

	text, err := decryptBuffer(key, cipherText)
	require.Nil(t, err)
	require.Equal(t, true, reflect.DeepEqual(text, plaintext))
}

func TestEncyptDecryptBufferKey32(t *testing.T) {
	plaintext := []byte("this is a test")

	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.Nil(t, err)

	cipherText, err := encryptBuffer(key, plaintext)
	require.Nil(t, err)

	text, err := decryptBuffer(key, cipherText)
	require.Nil(t, err)
	require.Equal(t, true, reflect.DeepEqual(text, plaintext))
}
