package memdx

import (
	"errors"
	"log"
	"sync"
)

// a basic memd client that provides opaque mapping and request dispatch...
// note that it is not thread-safe, but does use locks to prevent internal races
// between operations that are being sent and responses being received.
type Client struct {
	conn *Conn

	lock      sync.Mutex
	opaqueCtr uint32
	opaqueMap map[uint32]DispatchCallback
}

var _ Dispatcher = (*Client)(nil)

func Connect(address string) (*Client, error) {
	c := &Client{
		opaqueCtr: 1,
		opaqueMap: make(map[uint32]DispatchCallback),
	}
	go c.run()
	return c, nil
}

func (c *Client) run() {
	pak := &Packet{}
	for {
		err := c.conn.ReadPacket(pak)
		if err != nil {
			// TODO(brett19): Handle read errors...
			log.Printf("failed to read packet from socket: %s", err)
		}

		err = c.dispatchCallback(pak)
		if err != nil {
			// TODO(brett19): Handle dispatch errors
			log.Printf("failed to dispatch packet to handler: %s", err)
		}
	}
}

func (c *Client) registerHandler(handler DispatchCallback) uint32 {
	c.lock.Lock()

	opaqueID := c.opaqueCtr
	c.opaqueCtr++

	c.opaqueMap[opaqueID] = handler

	c.lock.Unlock()

	return opaqueID
}

func (c *Client) dispatchCallback(pak *Packet) error {
	c.lock.Lock()

	handler, handlerIsValid := c.opaqueMap[pak.Opaque]
	if !handlerIsValid {
		c.lock.Unlock()
		return errors.New("invalid opaque on response packet")
	}

	c.lock.Unlock()

	hasMorePackets := handler(pak, nil)

	if !hasMorePackets {
		c.lock.Lock()
		delete(c.opaqueMap, pak.Opaque)
		c.lock.Unlock()
	}

	return nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Dispatch(req *Packet, handler DispatchCallback) error {
	opaqueID := c.registerHandler(handler)
	req.Opaque = opaqueID
	return c.conn.WritePacket(req)
}
