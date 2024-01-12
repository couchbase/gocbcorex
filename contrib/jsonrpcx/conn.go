package jsonrpcx

import (
	"encoding/json"
	"errors"
	"io"
)

type Conn struct {
	conn   io.ReadWriteCloser
	reader *json.Decoder
	writer *json.Encoder
}

func NewConn(conn io.ReadWriteCloser) *Conn {
	return &Conn{
		conn:   conn,
		writer: json.NewEncoder(conn),
		reader: json.NewDecoder(conn),
	}
}

func (c *Conn) WriteRequest(req *Request) error {
	if req.Version == "" {
		req.Version = ProtoVersion
	} else if req.Version != ProtoVersion {
		return errors.New("cannot write messages with invalid jsonrpc version")
	}
	return c.writer.Encode(req)
}

func (c *Conn) WriteResponse(resp *Response) error {
	if resp.Version == "" {
		resp.Version = ProtoVersion
	} else if resp.Version != ProtoVersion {
		return errors.New("cannot write messages with invalid jsonrpc version")
	}
	return c.writer.Encode(resp)
}

func (c *Conn) ReadRequest(req *Request) error {
	err := c.reader.Decode(req)
	if err != nil {
		return err
	}

	if req.Version != ProtoVersion {
		return errors.New("invalid jsonrpc version")
	}

	return nil
}

func (c *Conn) ReadResponse(resp *Response) error {
	err := c.reader.Decode(resp)
	if err != nil {
		return err
	}

	if resp.Version != ProtoVersion {
		return errors.New("invalid jsonrpc version")
	}

	return nil
}

func (c *Conn) Close() error {
	return c.conn.Close()
}
