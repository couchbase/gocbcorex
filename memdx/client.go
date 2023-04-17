package memdx

import (
	"errors"
	"sync"

	"go.uber.org/zap"
)

// Client is a basic memd client that provides opaque mapping and request dispatch...
// note that it is not thread-safe, but does use locks to prevent internal races
// between operations that are being sent and responses being received.
type Client struct {
	conn          *Conn
	orphanHandler func(*Packet)
	closeHandler  func(error)
	logger        *zap.Logger

	// opaqueMapLock control access to the opaque map itself and is used for all access to it.
	opaqueMapLock sync.Mutex
	// handlerInvokeLock controls being able to call handlers from the opaque map, and is not used on the write side.
	// This allows us to coordinate between Close, cancelHandler, and dispatchCallback without having to lock for longer
	// in Dispatch.
	handlerInvokeLock sync.Mutex
	opaqueCtr         uint32
	opaqueMap         map[uint32]DispatchCallback
}

var _ Dispatcher = (*Client)(nil)

type ClientOptions struct {
	OrphanHandler func(*Packet)
	CloseHandler  func(error)
	Logger        *zap.Logger
}

func NewClient(conn *Conn, opts *ClientOptions) *Client {
	if opts == nil {
		opts = &ClientOptions{}
	}
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	c := &Client{
		conn:          conn,
		orphanHandler: opts.OrphanHandler,
		closeHandler:  opts.CloseHandler,
		logger:        logger,

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
			c.logger.Debug("failed to dispatch callback", zap.Error(err))
			closeErr = err
			break
		}
	}

	if c.closeHandler != nil {
		c.closeHandler(closeErr)
	}
}

func (c *Client) registerHandler(handler DispatchCallback) uint32 {
	c.opaqueMapLock.Lock()

	opaqueID := c.opaqueCtr
	c.opaqueCtr++

	c.opaqueMap[opaqueID] = handler

	c.opaqueMapLock.Unlock()

	return opaqueID
}

func (c *Client) cancelHandler(opaqueID uint32, err error) {
	c.handlerInvokeLock.Lock()
	defer c.handlerInvokeLock.Unlock()
	c.opaqueMapLock.Lock()

	handler, handlerIsValid := c.opaqueMap[opaqueID]
	if !handlerIsValid {
		c.opaqueMapLock.Unlock()
		return
	}

	delete(c.opaqueMap, opaqueID)
	c.opaqueMapLock.Unlock()

	c.logger.Debug("cancelling operation",
		zap.Uint32("opaque", opaqueID),
	)

	hasMorePackets := handler(nil, requestCancelledError{cause: err})
	if hasMorePackets {
		c.logger.DPanic("memd packet handler returned hasMorePackets after an error", zap.Uint32("opaque", opaqueID))
	}
}

func (c *Client) dispatchCallback(pak *Packet) error {
	c.handlerInvokeLock.Lock()
	defer c.handlerInvokeLock.Unlock()
	c.opaqueMapLock.Lock()

	handler, handlerIsValid := c.opaqueMap[pak.Opaque]
	if !handlerIsValid {
		orphanHandler := c.orphanHandler
		c.opaqueMapLock.Unlock()

		if orphanHandler == nil {
			return errors.New("invalid opaque on response packet")
		}

		orphanHandler(pak)
		return nil
	}
	c.opaqueMapLock.Unlock()

	hasMorePackets := handler(pak, nil)

	if !hasMorePackets {
		c.opaqueMapLock.Lock()
		delete(c.opaqueMap, pak.Opaque)
		c.opaqueMapLock.Unlock()
	}

	return nil
}

func (c *Client) Close() error {
	// Close will prevent any further writes or reads from occurring.
	// Writes will return an error. However, any already in flight ops will not
	// be handled by the read thread, so we need to iterate any handlers and
	// fail them.
	err := c.conn.Close()
	if err != nil {
		return err
	}

	c.handlerInvokeLock.Lock()
	c.opaqueMapLock.Lock()
	handlers := c.opaqueMap
	c.opaqueMap = map[uint32]DispatchCallback{}
	c.opaqueMapLock.Unlock()

	for _, handler := range handlers {
		handler(nil, ErrClosedInFlight)
	}

	c.handlerInvokeLock.Unlock()

	return nil
}

func (c *Client) Dispatch(req *Packet, handler DispatchCallback) (PendingOp, error) {
	opaqueID := c.registerHandler(handler)
	req.Opaque = opaqueID

	err := c.conn.WritePacket(req)
	if err != nil {
		c.logger.Debug("failed to write packet",
			zap.Error(err),
			zap.Uint32("opaque", opaqueID),
			zap.String("opcode", req.OpCode.String()),
		)
		c.opaqueMapLock.Lock()
		delete(c.opaqueMap, opaqueID)
		c.opaqueMapLock.Unlock()

		return nil, err
	}

	return clientPendingOp{
		client:   c,
		opaqueID: opaqueID,
	}, nil
}

func (c *Client) LocalAddr() string {
	return c.conn.LocalAddr()
}

func (c *Client) RemoteAddr() string {
	return c.conn.RemoteAddr()
}
