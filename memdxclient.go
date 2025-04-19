package gocbcorex

import (
	"net"

	"github.com/couchbase/gocbcorex/memdx"
)

type MemdxClient interface {
	memdx.Dispatcher
	WritePacket(pak *memdx.Packet) error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Close() error
}

var _ MemdxClient = (*memdx.Client)(nil)
