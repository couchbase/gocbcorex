package memdx

import "net"

type Conn struct {
	conn net.Conn
}

func (c *Conn) WritePacket(pak *Packet) error {
	return nil
}

func (c *Conn) ReadPacket(pak *Packet) error {
	return nil
}

func (c *Conn) Close() error {
	return c.conn.Close()
}
