package memdx

import (
	"errors"
	"sync"
)

// a basic memd client that provides opaque mapping and request dispatch...
// note that it is not thread-safe, but does use locks to prevent internal races
// between operations that are being sent and responses being received.
type Client struct {
	conn          *Conn
	orphanHandler func(*Packet)
	closeHandler  func(error)

	lock      sync.Mutex
	opaqueCtr uint32
	opaqueMap map[uint32]DispatchCallback
}

var _ Dispatcher = (*Client)(nil)

type ClientOptions struct {
	OrphanHandler func(*Packet)
	CloseHandler  func(error)
}

func NewClient(conn *Conn, opts *ClientOptions) *Client {
	if opts == nil {
		opts = &ClientOptions{}
	}

	c := &Client{
		conn:          conn,
		orphanHandler: opts.OrphanHandler,
		closeHandler:  opts.CloseHandler,

		opaqueCtr: 1,
		opaqueMap: make(map[uint32]DispatchCallback),
	}
	go c.run()

	return c
}

func (c *Client) run() {
	pak := &Packet{}
	var closeErr error
	for {
		err := c.conn.ReadPacket(pak)
		if err != nil {
			closeErr = err
			break
		}

		err = c.dispatchCallback(pak)
		if err != nil {
			closeErr = err
			break
		}
	}

	if closeErr != nil {
		if c.closeHandler != nil {
			c.closeHandler(closeErr)
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
		orphanHandler := c.orphanHandler
		c.lock.Unlock()

		if orphanHandler == nil {
			return errors.New("invalid opaque on response packet")
		}

		orphanHandler(pak)
		return nil
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