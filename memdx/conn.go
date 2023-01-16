package memdx

import (
	"net"
)

type Conn struct {
	conn net.Conn

	reader PacketReader
	writer PacketWriter
}

func NewConn(conn net.Conn) *Conn {
	return &Conn{
		conn: conn,
	}
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
