package memdx

import (
	"errors"
	"net"
	"os"
	"sync"

	"go.uber.org/zap"
)

var enablePacketLogging bool = os.Getenv("GCBCX_PACKET_LOGGING") != ""

// Client is a basic memd client that provides opaque mapping and request dispatch...
// note that it is not thread-safe, but does use locks to prevent internal races
// between operations that are being sent and responses being received.
type Client struct {
	conn               *Conn
	unsolicitedHandler func(*Packet)
	orphanHandler      func(*Packet)
	closeHandler       func(error)
	logger             *zap.Logger

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
	UnsolicitedHandler func(*Packet)
	OrphanHandler      func(*Packet)
	CloseHandler       func(error)
	Logger             *zap.Logger
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
		conn:               conn,
		unsolicitedHandler: opts.UnsolicitedHandler,
		orphanHandler:      opts.OrphanHandler,
		closeHandler:       opts.CloseHandler,
		logger:             logger,

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

	hasMorePackets := handler(nil, &requestCancelledError{cause: err})
	if hasMorePackets {
		c.logger.DPanic("memd packet handler returned hasMorePackets after an error", zap.Uint32("opaque", opaqueID))
	}
}

func (c *Client) dispatchCallback(pak *Packet) error {
	if enablePacketLogging {
		c.logger.Debug("read packet",
			zap.String("magic", pak.Magic.String()),
			zap.String("opcode", pak.OpCode.String(pak.Magic)),
			zap.Uint8("datatype", pak.Datatype),
			zap.Uint16("vbucketID", pak.VbucketID),
			zap.String("status", pak.Status.String()),
			zap.Uint32("opaque", pak.Opaque),
			zap.Uint64("cas", pak.Cas),
			zap.Binary("extras", pak.Extras),
			zap.Binary("key", pak.Key),
			zap.Binary("value", pak.Value),
			zap.Binary("framingExtras", pak.FramingExtras),
		)
	}

	c.handlerInvokeLock.Lock()
	defer c.handlerInvokeLock.Unlock()

	if pak.Magic.IsRequest() {
		unsolicitedHandler := c.unsolicitedHandler

		if unsolicitedHandler == nil {
			return errors.New("unexpected unsolicited packet")
		}

		unsolicitedHandler(pak)
		return nil
	}

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

func (c *Client) WritePacket(pak *Packet) error {
	return c.conn.WritePacket(pak)
}

// Dispatches a packet to the network, calling the handler with responses.
// Note that the handlers can be invoked before this function returns due to races
// between this function returning and the IO thread receiving responses. This out
// of order invocation can also happen in cancel cases.  You are guarenteed however
// to either receive callbacks OR receive an error from this call, never both.
func (c *Client) Dispatch(req *Packet, handler DispatchCallback) (PendingOp, error) {
	opaqueID := c.registerHandler(handler)
	req.Opaque = opaqueID

	if enablePacketLogging {
		c.logger.Debug("writing packet",
			zap.String("magic", req.Magic.String()),
			zap.String("opcode", req.OpCode.String(req.Magic)),
			zap.Uint8("datatype", req.Datatype),
			zap.Uint16("vbucketID", req.VbucketID),
			zap.String("status", req.Status.String()),
			zap.Uint32("opaque", req.Opaque),
			zap.Uint64("cas", req.Cas),
			zap.Binary("extras", req.Extras),
			zap.Binary("key", req.Key),
			zap.Binary("value", req.Value),
			zap.Binary("framingExtras", req.FramingExtras),
		)
	}

	err := c.conn.WritePacket(req)
	if err != nil {
		c.logger.Debug("failed to write packet",
			zap.Error(err),
			zap.Uint32("opaque", opaqueID),
			zap.String("opcode", req.OpCode.String(req.Magic)),
		)

		c.opaqueMapLock.Lock()
		if _, ok := c.opaqueMap[opaqueID]; !ok {
			// if the handler isn't in the opaque map anymore, we can assume that
			// we've been cancelled by someone while we were waiting for the Write.
			// We pretend that the write was successful in this case, since the
			// callback will already have been invoked with errors by someone else.
			c.opaqueMapLock.Unlock()
			return PendingOpNoop{}, nil
		}

		delete(c.opaqueMap, opaqueID)
		c.opaqueMapLock.Unlock()

		return nil, err
	}

	return clientPendingOp{
		client:   c,
		opaqueID: opaqueID,
	}, nil
}

func (c *Client) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Client) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}
