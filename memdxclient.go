package gocbcorex

import (
	"net"

	"github.com/couchbase/gocbcorex/memdx"
)

type MemdxClient interface {
	memdx.Dispatcher
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Close() error
}

var _ MemdxClient = (*memdx.Client)(nil)
