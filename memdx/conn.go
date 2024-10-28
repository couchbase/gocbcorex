package memdx

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
)

type Conn struct {
	conn     net.Conn
	ioReader io.Reader

	reader PacketReader
	writer PacketWriter
}

type DialConnOptions struct {
	TLSConfig *tls.Config
	Dialer    *net.Dialer

	ReadBufferSize int
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

	// we disable TCP_NODELAY by default, this turns on the NAGLE algorithm and makes our
	// network writes more efficient, in the future this should be done in user-space.
	_ = tcpConn.SetNoDelay(false)

	// if there is a read buffer size configured, we use it
	var ioReader io.Reader
	if readBufferSize > 0 {
		ioReader = bufio.NewReaderSize(netConn, readBufferSize)
	} else {
		ioReader = netConn
	}

	c := &Conn{
		conn:     netConn,
		ioReader: ioReader,
	}

	return c, nil
}

func (c *Conn) WritePacket(pak *Packet) error {
	return c.writer.WritePacket(c.conn, pak)
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
