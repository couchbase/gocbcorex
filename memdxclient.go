package gocbcorex

import (
	"context"
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

func DialMemdxClient(
	ctx context.Context,
	address string,
	dialOpts *memdx.DialConnOptions,
	clientOpts *memdx.ClientOptions,
) (MemdxClient, error) {
	conn, err := memdx.DialConn(ctx, address, dialOpts)
	if err != nil {
		return nil, err
	}

	return memdx.NewClient(conn, clientOpts), nil
}
