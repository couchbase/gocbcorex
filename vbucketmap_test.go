package gocbcorex

import (
	"errors"
	"reflect"
	"testing"
)

type test struct {
	input  []byte
	output uint16
	err    error
}

func executeTests(t *testing.T, tests []test, vbMap *VbucketMap) {
	for _, tc := range tests {
		got, err := vbMap.VbucketByKey(tc.input)
		if !reflect.DeepEqual(tc.output, got) {
			t.Fatalf("expected: %v, got: %v", tc.output, got)
		}
		if !errors.Is(tc.err, err) {
			t.Fatalf("expected err: %v, got: %v", tc.err, err)
		}
	}
}

func TestVbucketMapWith0Vbs(t *testing.T) {
	vbMap := NewVbucketMap(make([][]int, 0), 1)
	tests := []test{
		{input: []byte{0}, output: uint16(0x0), err: ErrVbucketMapEntriesEmpty},
		{input: []byte{0, 1, 2, 3, 4, 5, 6, 7}, output: uint16(0x0), err: ErrVbucketMapEntriesEmpty},
		{input: []byte("hello"), output: uint16(0x0), err: ErrVbucketMapEntriesEmpty},
		{input: []byte("hello world, I am a super long key lets see if it works"), output: uint16(0x0), err: ErrVbucketMapEntriesEmpty},
	}
	executeTests(t, tests, vbMap)
}

func TestVbucketMapWith1024Vbs(t *testing.T) {
	vbMap := NewVbucketMap(make([][]int, 1024), 1)
	tests := []test{
		{input: []byte{0}, output: uint16(0x0202), err: nil},
		{input: []byte{0, 1, 2, 3, 4, 5, 6, 7}, output: uint16(0x00aa), err: nil},
		{input: []byte("hello"), output: uint16(0x0210), err: nil},
		{input: []byte("hello world, I am a super long key lets see if it works"), output: uint16(0x03d4), err: nil},
	}
	executeTests(t, tests, vbMap)
}

func TestVbucketMapWith64Vbs(t *testing.T) {
	vbMap := NewVbucketMap(make([][]int, 64), 1)
	tests := []test{
		{input: []byte{0}, output: uint16(0x0002), err: nil},
		{input: []byte{0, 1, 2, 3, 4, 5, 6, 7}, output: uint16(0x002a), err: nil},
		{input: []byte("hello"), output: uint16(0x0010), err: nil},
		{input: []byte("hello world, I am a super long key lets see if it works"), output: uint16(0x0014), err: nil},
	}
	executeTests(t, tests, vbMap)
}

func TestVbucketMapWith48Vbs(t *testing.T) {
	vbMap := NewVbucketMap(make([][]int, 48), 1)
	tests := []test{
		{input: []byte{0}, output: uint16(0x0012), err: nil},
		{input: []byte{0, 1, 2, 3, 4, 5, 6, 7}, output: uint16(0x000a), err: nil},
		{input: []byte("hello"), output: uint16(0x0010), err: nil},
		{input: []byte("hello world, I am a super long key lets see if it works"), output: uint16(0x0004), err: nil},
	}
	executeTests(t, tests, vbMap)
}

func TestVbucketMapWith13Vbs(t *testing.T) {
	vbMap := NewVbucketMap(make([][]int, 13), 1)
	tests := []test{
		{input: []byte{0}, output: uint16(0x000c), err: nil},
		{input: []byte{0, 1, 2, 3, 4, 5, 6, 7}, output: uint16(0x0008), err: nil},
		{input: []byte("hello"), output: uint16(0x0008), err: nil},
		{input: []byte("hello world, I am a super long key lets see if it works"), output: uint16(0x0003), err: nil},
	}
	executeTests(t, tests, vbMap)
}
