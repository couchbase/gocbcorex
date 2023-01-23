package memdx

import (
	"context"
	"crypto/tls"
	"net"
)

type Conn struct {
	conn net.Conn

	reader PacketReader
	writer PacketWriter
}

type DialConnOptions struct {
	TLSConfig *tls.Config
	Dialer    *net.Dialer
}

func DialConn(ctx context.Context, addr string, opts *DialConnOptions) (*Conn, error) {
	if opts == nil {
		opts = &DialConnOptions{}
	}

	dialer := opts.Dialer
	if dialer == nil {
		dialer = &net.Dialer{}
	}

	var netConn net.Conn
	if opts.TLSConfig == nil {
		tcpConn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}

		netConn = tcpConn
	} else {
		tlsConn, err := tls.DialWithDialer(dialer, "tcp", addr, opts.TLSConfig)
		if err != nil {
			return nil, err
		}

		netConn = tlsConn
	}

	return &Conn{
		conn: netConn,
	}, nil
}

func (c *Conn) WritePacket(pak *Packet) error {
	return c.writer.WritePacket(c.conn, pak)
}

func (c *Conn) ReadPacket(pak *Packet) error {
	return c.reader.ReadPacket(c.conn, pak)
}

func (c *Conn) Close() error {
	return c.conn.Close()
}
