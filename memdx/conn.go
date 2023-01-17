package memdx

import (
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
}

func DialConn(addr string, opts *DialConnOptions) (*Conn, error) {
	if opts == nil {
		opts = &DialConnOptions{}
	}

	var netConn net.Conn
	if opts.TLSConfig == nil {
		tcpConn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}

		netConn = tcpConn
	} else {
		tlsConn, err := tls.Dial("tcp", addr, opts.TLSConfig)
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
