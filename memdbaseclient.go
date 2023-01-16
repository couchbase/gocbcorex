package core

import (
	"net"
)

type memdBaseClient struct {
	conn net.Conn
}

type MemdDispatchCallback func(resp *memdRawPacket, err error) bool

func (c *memdBaseClient) Dispatch(req *memdRawPacket, handler MemdDispatchCallback) error {
	return nil
}
