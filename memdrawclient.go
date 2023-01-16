package core

import (
	"errors"
	"net"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/stellar-nebula/core/memdx"
)

type PacketType uint

const (
	PacketTypeUnknown PacketType = iota
	PacketTypeReq
	PacketTypeRes
)

type memdRawPacket struct {
	PacketType    PacketType
	Command       memd.CmdCode
	Datatype      uint8
	VbucketID     uint16 // Only valid for Req type packets
	Status        uint16 // Only valid for Resp type packets
	Opaque        uint32
	Cas           uint64
	Extras        []byte
	Key           []byte
	Value         []byte
	FramingExtras []byte
}

type memdRawClient struct {
	conn net.Conn

	// writeBuf represents a temporary buffer that is used to store the
	// binary format of packets before they are written to the operating
	// system.
	writeBuf []byte

	// readBuf represents a temporary buffer used to read packets.
	readBuf []byte
}

func (c *memdBaseClient) WritePacket(pak *memdRawPacket) error {
	if pak.PacketType == PacketTypeReq {
		if len(pak.FramingExtras) > 0 {
			magic := memdx.MagicReqExt
		} else {
			magic := memdx.MagicReq
		}
	} else if pak.PacketType == PacketTypeRes {
		if len(pak.FramingExtras) > 0 {
			magic := memdx.MagicResExt
		} else {
			magic := memdx.MagicRes
		}
	} else {
		return errors.New("invalid packet type")
	}
}

func (c *memdBaseClient) ReadPacket(pak *memdRawPacket) error {
	return nil
}
