package memdx

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

type Conn struct {
	conn          net.Conn
	ioReader      io.Reader
	ioWriter      io.Writer
	bufWriter     *bufio.Writer
	writeLock     sync.Mutex
	pendingWrites int64

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
		readBufferSize = 1 * 1024 * 1024
	}

	writeBufferSize := opts.WriteBufferSize
	if writeBufferSize == 0 {
		// default to a write-buffer size of 64KB, use -1 to disable
		writeBufferSize = 64 * 1024
	}

	dialer := opts.Dialer
	if dialer == nil {
		dialer = &net.Dialer{}
	}

	var netConn net.Conn
	if opts.TLSConfig == nil {
		dialConn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}

		netConn = dialConn
	} else {
		tlsConn, err := tls.DialWithDialer(dialer, "tcp", addr, opts.TLSConfig)
		if err != nil {
			return nil, err
		}

		netConn = tlsConn
	}

	// if there is a read buffer size configured, we use it
	var ioReader io.Reader
	if readBufferSize > 0 {
		ioReader = bufio.NewReaderSize(netConn, readBufferSize)
	} else {
		ioReader = netConn
	}

	var ioWriter io.Writer
	var bufWriter *bufio.Writer
	if writeBufferSize > 0 {
		bufWriter = bufio.NewWriterSize(netConn, writeBufferSize)
		ioWriter = bufWriter
	} else {
		ioWriter = netConn
	}

	c := &Conn{
		conn:      netConn,
		ioReader:  ioReader,
		ioWriter:  ioWriter,
		bufWriter: bufWriter,
	}

	return c, nil
}

func (c *Conn) WritePacket(pak *Packet) error {
	atomic.AddInt64(&c.pendingWrites, 1)

	c.writeLock.Lock()
	err := c.writer.WritePacket(c.ioWriter, pak)

	pendingWrites := atomic.AddInt64(&c.pendingWrites, -1)
	if pendingWrites == 0 {
		if c.bufWriter != nil {
			_ = c.bufWriter.Flush()
		}
	}

	c.writeLock.Unlock()

	return err
}

func (c *Conn) ReadPacket(pak *Packet) error {
	return c.reader.ReadPacket(c.ioReader, pak)
}

func (c *Conn) Close() error {
	return c.conn.Close()
}

func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}
