package memdx

import (
	"encoding/binary"
	"io"
	"math"
)

type PacketWriter struct {
	// we use a heap-allocated write buffer since io.Write will cause
	// the buffer to escape regardless of what we want.
	writeBuf []byte
}

func (pw *PacketWriter) WritePacket(w io.Writer, pak *Packet) error {
	extFramesLen := len(pak.FramingExtras)
	extrasLen := len(pak.Extras)
	keyLen := len(pak.Key)
	valueLen := len(pak.Value)
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
	if cap(pw.writeBuf) < totalLen {
		pw.writeBuf = make([]byte, totalLen)
	}

	// build the packet in the write buffer
	pw.writeBuf = pw.writeBuf[:0]
	pw.writeBuf = append(pw.writeBuf, headerBuf...)
	pw.writeBuf = append(pw.writeBuf, pak.FramingExtras...)
	pw.writeBuf = append(pw.writeBuf, pak.Extras...)
	pw.writeBuf = append(pw.writeBuf, pak.Key...)
	pw.writeBuf = append(pw.writeBuf, pak.Value...)

	// Write guarentees that err is returned if n<len, so we can just ignore
	// n and only inspect the error to determine if something went wrong...
	_, err := w.Write(pw.writeBuf)
	if err != nil {
		return err
	}

	return nil
}
