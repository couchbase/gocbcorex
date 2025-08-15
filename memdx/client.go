package memdx

import (
	"errors"
	"net"
	"os"

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
	readErrorHandler   func(error)
	logger             *zap.Logger

	opaqueMap *OpaqueMap
}

var _ Dispatcher = (*Client)(nil)

type ClientOptions struct {
	UnsolicitedHandler func(*Packet)
	OrphanHandler      func(*Packet)
	ReadErrorHandler   func(error)
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
		readErrorHandler:   opts.ReadErrorHandler,
		logger:             logger,

		opaqueMap: NewOpaqueMap(),
	}
	go c.run()

	return c
}

func (c *Client) run() {
	pak := &Packet{}
	var readErr error
	for {
		err := c.conn.ReadPacket(pak)
		if err != nil {
			readErr = err
			break
		}

		err = c.dispatchCallback(pak)
		if err != nil {
			c.logger.Debug("failed to dispatch callback", zap.Error(err))
			readErr = err
			break
		}
	}

	if c.readErrorHandler != nil {
		c.readErrorHandler(readErr)
	}

	c.opaqueMap.CancelAll(ErrClosedInFlight)
}

func (c *Client) cancelOp(opaqueID uint32, err error) bool {
	c.logger.Debug("cancelling operation",
		zap.Uint32("opaque", opaqueID),
	)

	hasMorePackets, wasInvoked := c.opaqueMap.Invoke(opaqueID, nil, err)
	if hasMorePackets {
		c.logger.DPanic("memd packet handler returned hasMorePackets after an error", zap.Uint32("opaque", opaqueID))
	}

	return wasInvoked
}

func (c *Client) dispatchCallback(pak *Packet) error {
	if enablePacketLogging {
		c.logger.Debug("read packet",
			zap.Bool("isResponse", pak.IsResponse),
			zap.String("opcode", pak.OpCode.String()),
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

	if !pak.IsResponse {
		unsolicitedHandler := c.unsolicitedHandler

		if unsolicitedHandler == nil {
			return errors.New("unexpected unsolicited packet")
		}

		unsolicitedHandler(pak)
		return nil
	}

	_, wasInvoked := c.opaqueMap.Invoke(pak.Opaque, pak, nil)
	if !wasInvoked {
		orphanHandler := c.orphanHandler

		if orphanHandler == nil {
			return errors.New("invalid opaque on response packet")
		}

		orphanHandler(pak)
		return nil
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
	opaqueID := c.opaqueMap.Register(handler)
	req.Opaque = opaqueID

	if enablePacketLogging {
		c.logger.Debug("writing packet",
			zap.Bool("isResponse", req.IsResponse),
			zap.String("opcode", req.OpCode.String()),
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
			zap.String("opcode", req.OpCode.String()),
		)

		wasInvalidated := c.opaqueMap.Invalidate(opaqueID)
		if !wasInvalidated {
			// if we fail to invalidate this entry, we can assume that the handler
			// has already been invoked by a cancellation, and thus we pretend that
			// the write was successful to maintain our guarantee about error and
			// callback invocation.
			return PendingOpNoop{}, nil
		}

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
