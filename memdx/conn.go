package memdx

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"

	"github.com/couchbase/gocbcorex/contrib/asyncwritebuf"
)

type Conn struct {
	conn     net.Conn
	ioReader io.Reader
	ioWriter io.Writer

	asyncWriter *asyncwritebuf.Writer

	reader PacketReader
	writer PacketWriter
}

type DialConnOptions struct {
	TLSConfig *tls.Config
	Dialer    *net.Dialer

	ReadBufferSize  int
	WriteBufferSize int
}

func DialConn(ctx context.Context, addr string, opts *DialConnOptions) (*Conn, error) {
	if opts == nil {
		opts = &DialConnOptions{}
	}

	readBufferSize := opts.ReadBufferSize
	if readBufferSize == 0 {
		// default to a read-buffer size of 1MB, use -1 to disable
		readBufferSize = 10 * 1024 * 1024
	}

	writeBufferSize := opts.WriteBufferSize
	if writeBufferSize == 0 {
		// default to a read-buffer size of 1MB, use -1 to disable
		writeBufferSize = 0
	}

	dialer := opts.Dialer
	if dialer == nil {
		dialer = &net.Dialer{}
	}

	var netConn net.Conn
	var tcpConn *net.TCPConn
	if opts.TLSConfig == nil {
		dialConn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}

		netConn = dialConn
		tcpConn, _ = dialConn.(*net.TCPConn)
	} else {
		tlsConn, err := tls.DialWithDialer(dialer, "tcp", addr, opts.TLSConfig)
		if err != nil {
			return nil, err
		}

		netConn = tlsConn
		tcpConn, _ = tlsConn.NetConn().(*net.TCPConn)
	}

	// if there is a read buffer size configured, we use it
	var ioReader io.Reader
	if readBufferSize > 0 {
		ioReader = bufio.NewReaderSize(netConn, readBufferSize)
	} else {
		ioReader = netConn
	}

	var asyncWriter *asyncwritebuf.Writer
	var ioWriter io.Writer
	if writeBufferSize > 0 {
		asyncWriter = asyncwritebuf.NewWriter(netConn, writeBufferSize)
		ioWriter = asyncWriter
	} else {
		ioWriter = netConn

		// we disable TCP_NODELAY by default, this turns on the NAGLE algorithm and makes our
		// network writes more efficient, this is only needed when we don't have user-space
		// buffering to aggregate writes with.
		_ = tcpConn.SetNoDelay(false)
	}

	c := &Conn{
		conn:        netConn,
		ioReader:    ioReader,
		ioWriter:    ioWriter,
		asyncWriter: asyncWriter,
	}

	return c, nil
}

func (c *Conn) WritePacket(pak *Packet) error {
	return c.writer.WritePacket(c.ioWriter, pak)
}

func (c *Conn) ReadPacket(pak *Packet) error {
	return c.reader.ReadPacket(c.ioReader, pak)
}

func (c *Conn) Close() error {
	if c.asyncWriter != nil {
		c.asyncWriter.Close()
	}

	return c.conn.Close()
}

func (c *Conn) LocalAddr() string {
	return c.conn.LocalAddr().String()
}

func (c *Conn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}
