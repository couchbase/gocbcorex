package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVbucketMapWith1024Vbs(t *testing.T) {
	vbMap := newVbucketMap(make([][]int, 1024), 1)
	require.Equal(t, uint16(0x0202), vbMap.VbucketByKey([]byte{0}))
	require.Equal(t, uint16(0x00aa), vbMap.VbucketByKey([]byte{0, 1, 2, 3, 4, 5, 6, 7}))
	require.Equal(t, uint16(0x0210), vbMap.VbucketByKey([]byte("hello")))
	require.Equal(t, uint16(0x03d4), vbMap.VbucketByKey([]byte("hello world, I am a super long key lets see if it works")))
}

func TestVbucketMapWith64Vbs(t *testing.T) {
	vbMap := newVbucketMap(make([][]int, 64), 1)
	require.Equal(t, uint16(0x0002), vbMap.VbucketByKey([]byte{0}))
	require.Equal(t, uint16(0x002a), vbMap.VbucketByKey([]byte{0, 1, 2, 3, 4, 5, 6, 7}))
	require.Equal(t, uint16(0x0010), vbMap.VbucketByKey([]byte("hello")))
	require.Equal(t, uint16(0x0014), vbMap.VbucketByKey([]byte("hello world, I am a super long key lets see if it works")))
}

func TestVbucketMapWith48Vbs(t *testing.T) {
	vbMap := newVbucketMap(make([][]int, 48), 1)
	require.Equal(t, uint16(0x0012), vbMap.VbucketByKey([]byte{0}))
	require.Equal(t, uint16(0x000a), vbMap.VbucketByKey([]byte{0, 1, 2, 3, 4, 5, 6, 7}))
	require.Equal(t, uint16(0x0010), vbMap.VbucketByKey([]byte("hello")))
	require.Equal(t, uint16(0x0004), vbMap.VbucketByKey([]byte("hello world, I am a super long key lets see if it works")))
}

func TestVbucketMapWith13Vbs(t *testing.T) {
	vbMap := newVbucketMap(make([][]int, 13), 1)
	require.Equal(t, uint16(0x000c), vbMap.VbucketByKey([]byte{0}))
	require.Equal(t, uint16(0x0008), vbMap.VbucketByKey([]byte{0, 1, 2, 3, 4, 5, 6, 7}))
	require.Equal(t, uint16(0x0008), vbMap.VbucketByKey([]byte("hello")))
	require.Equal(t, uint16(0x0003), vbMap.VbucketByKey([]byte("hello world, I am a super long key lets see if it works")))
}
