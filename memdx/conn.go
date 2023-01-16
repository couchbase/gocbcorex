package memdx

import (
	"encoding/binary"
	"io"
	"math"
	"net"
)

type Conn struct {
	conn net.Conn

	// we use a heap-allocated write buffer since io.Write will cause
	// the buffer to escape regardless of what we want.
	writeBuf []byte

	// we use this heap-allocated read buffer since io.Read will cause
	// the buffer to escape.  the payload portion of the packet is
	// allocated on-demand since it will _always_ escape through references
	// that existing in the *Packet object.
	readHeaderBuf []byte
}

func NewConn(conn net.Conn) *Conn {
	return &Conn{
		conn: conn,
	}
}

func (c *Conn) WritePacket(pak *Packet) error {
	extFramesLen := len(pak.FramingExtras)
	extrasLen := len(pak.Extras)
	keyLen := len(pak.Key)
	valueLen := len(pak.Key)
	payloadLen := extFramesLen + extrasLen + keyLen + valueLen
	totalLen := 24 + payloadLen

	// we intentionally guarentee that headerBuf never escapes this function
	// so this will end up not needing to actually allocate (will go on stack)
	headerBuf := make([]byte, 24)

	headerBuf[0] = uint8(pak.Magic)
	headerBuf[1] = uint8(pak.OpCode)

	if pak.Magic == MagicReq || pak.Magic == MagicRes {
		if extFramesLen > 0 {
			return protocolError{"cannot use framing extras with non-ext packets"}
		}

		if keyLen > math.MaxUint16 {
			return protocolError{"key too long to encode"}
		}

		binary.BigEndian.PutUint16(headerBuf[2:], uint16(keyLen))
	} else if pak.Magic == MagicReqExt || pak.Magic == MagicResExt {
		if extFramesLen > math.MaxUint8 {
			return protocolError{"framing extras too long to encode"}
		}

		if keyLen > math.MaxUint8 {
			return protocolError{"key too long to encode"}
		}

		headerBuf[2] = uint8(extFramesLen)
		headerBuf[3] = uint8(keyLen)
	} else {
		return protocolError{"invalid magic for key length encoding"}
	}

	if extrasLen > math.MaxUint8 {
		return protocolError{"extras too long to encode"}
	}
	headerBuf[4] = uint8(extrasLen)

	headerBuf[5] = pak.Datatype

	if pak.Magic == MagicReq || pak.Magic == MagicReqExt {
		if pak.Status != 0 {
			return protocolError{"cannot specify status in a request packet"}
		}

		binary.BigEndian.PutUint16(headerBuf[6:], pak.VbucketID)
	} else if pak.Magic == MagicRes || pak.Magic == MagicResExt {
		if pak.VbucketID != 0 {
			return protocolError{"cannot specify vbucket in a response packet"}
		}

		binary.BigEndian.PutUint16(headerBuf[6:], uint16(pak.Status))
	} else {
		return protocolError{"invalid magic for status/vbucket encoding"}
	}

	if payloadLen > math.MaxUint32 {
		return protocolError{"packet too long to encode"}
	}
	binary.BigEndian.PutUint32(headerBuf[8:], uint32(payloadLen))

	binary.BigEndian.PutUint32(headerBuf[12:], pak.Opaque)

	binary.BigEndian.PutUint64(headerBuf[16:], pak.Cas)

	// if the writeBuf isn't big enough, do a single resize so
	// we dont incrementally increase its size on each append.
	if cap(c.writeBuf) < totalLen {
		c.writeBuf = make([]byte, totalLen)
	}

	// build the packet in the write buffer
	c.writeBuf = c.writeBuf[:0]
	c.writeBuf = append(c.writeBuf, headerBuf...)
	c.writeBuf = append(c.writeBuf, pak.FramingExtras...)
	c.writeBuf = append(c.writeBuf, pak.Extras...)
	c.writeBuf = append(c.writeBuf, pak.Key...)
	c.writeBuf = append(c.writeBuf, pak.Value...)

	// Write guarentees that err is returned if n<len, so we can just ignore
	// n and only inspect the error to determine if something went wrong...
	_, err := c.conn.Write(c.writeBuf)
	if err != nil {
		return err
	}

	return nil
}

func (c *Conn) ReadPacket(pak *Packet) error {
	if len(c.readHeaderBuf) != 24 {
		c.readHeaderBuf = make([]byte, 24)
	}
	headerBuf := c.readHeaderBuf

	_, err := io.ReadFull(c.conn, headerBuf)
	if err != nil {
		return err
	}

	pak.Magic = Magic(headerBuf[0])
	pak.OpCode = OpCode(headerBuf[1])

	var extFramesLen int
	var keyLen int
	if pak.Magic == MagicReq || pak.Magic == MagicRes {
		extFramesLen = 0
		keyLen = int(binary.BigEndian.Uint16(headerBuf[2:]))
	} else if pak.Magic == MagicReqExt || pak.Magic == MagicResExt {
		extFramesLen = int(headerBuf[2])
		keyLen = int(headerBuf[3])
	} else {
		return protocolError{"invalid magic for key length decoding"}
	}

	extrasLen := int(headerBuf[4])

	pak.Datatype = headerBuf[5]

	if pak.Magic == MagicReq || pak.Magic == MagicReqExt {
		pak.VbucketID = binary.BigEndian.Uint16(headerBuf[6:])
		pak.Status = 0
	} else if pak.Magic == MagicRes || pak.Magic == MagicResExt {
		pak.VbucketID = 0
		pak.Status = Status(binary.BigEndian.Uint16(headerBuf[6:]))
	} else {
		return protocolError{"invalid magic for status/vbucket decoding"}
	}

	payloadLen := int(binary.BigEndian.Uint32(headerBuf[8:]))

	pak.Opaque = binary.BigEndian.Uint32(headerBuf[12:])

	pak.Cas = binary.BigEndian.Uint64(headerBuf[16:])

	valueLen := payloadLen - extFramesLen - extrasLen - keyLen

	// we intentionally put the payload in a newly allocated buffer because
	// it inevitably is going to escape to the heap through the Packet anyways.
	payloadBuf := make([]byte, payloadLen)
	_, err = io.ReadFull(c.conn, payloadBuf)
	if err != nil {
		return err
	}

	payloadPos := 0

	pak.FramingExtras = payloadBuf[payloadPos : payloadPos+extFramesLen]
	payloadPos += extFramesLen

	pak.Extras = payloadBuf[payloadPos : payloadPos+extrasLen]
	payloadPos += extrasLen

	pak.Key = payloadBuf[payloadPos : payloadPos+keyLen]
	payloadPos += keyLen

	pak.Value = payloadBuf[payloadPos : payloadPos+valueLen]
	payloadPos += valueLen

	return nil
}

func (c *Conn) Close() error {
	return c.conn.Close()
}
