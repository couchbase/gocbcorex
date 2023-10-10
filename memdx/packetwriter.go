package memdx

import (
	"encoding/binary"
	"io"
	"math"
)

type PacketWriter struct {
}

func (pw *PacketWriter) WritePacket(w io.Writer, pak *Packet) error {
	extFramesLen := len(pak.FramingExtras)
	extrasLen := len(pak.Extras)
	keyLen := len(pak.Key)
	valueLen := len(pak.Value)
	payloadLen := extFramesLen + extrasLen + keyLen + valueLen
	totalLen := 24 + payloadLen

	// we intentionally guarantee that headerBuf never escapes this function
	// so this will end up not needing to actually allocate (will go on stack)
	headerBuf := make([]byte, 24)

	headerBuf[0] = uint8(pak.Magic)
	headerBuf[1] = uint8(pak.OpCode)

	if pak.Magic == MagicReq || pak.Magic == MagicRes {
		if extFramesLen > 0 {
			return invalidArgError{"cannot use framing extras with non-ext packets"}
		}

		if keyLen > math.MaxUint16 {
			return invalidArgError{"key too long to encode"}
		}

		binary.BigEndian.PutUint16(headerBuf[2:], uint16(keyLen))
	} else if pak.Magic == MagicReqExt || pak.Magic == MagicResExt {
		if extFramesLen > math.MaxUint8 {
			return invalidArgError{"framing extras too long to encode"}
		}

		if keyLen > math.MaxUint8 {
			return invalidArgError{"key too long to encode"}
		}

		headerBuf[2] = uint8(extFramesLen)
		headerBuf[3] = uint8(keyLen)
	} else {
		return invalidArgError{"invalid magic for key length encoding"}
	}

	if extrasLen > math.MaxUint8 {
		return invalidArgError{"extras too long to encode"}
	}
	headerBuf[4] = uint8(extrasLen)

	headerBuf[5] = pak.Datatype

	if pak.Magic == MagicReq || pak.Magic == MagicReqExt {
		if pak.Status != 0 {
			return invalidArgError{"cannot specify status in a request packet"}
		}

		binary.BigEndian.PutUint16(headerBuf[6:], pak.VbucketID)
	} else if pak.Magic == MagicRes || pak.Magic == MagicResExt {
		if pak.VbucketID != 0 {
			return invalidArgError{"cannot specify vbucket in a response packet"}
		}

		binary.BigEndian.PutUint16(headerBuf[6:], uint16(pak.Status))
	} else {
		return invalidArgError{"invalid magic for status/vbucket encoding"}
	}

	if payloadLen > math.MaxUint32 {
		return invalidArgError{"packet too long to encode"}
	}
	binary.BigEndian.PutUint32(headerBuf[8:], uint32(payloadLen))

	binary.BigEndian.PutUint32(headerBuf[12:], pak.Opaque)

	binary.BigEndian.PutUint64(headerBuf[16:], pak.Cas)

	// build the packet in the write buffer
	writeBuf := make([]byte, 0, totalLen)
	writeBuf = append(writeBuf, headerBuf...)
	writeBuf = append(writeBuf, pak.FramingExtras...)
	writeBuf = append(writeBuf, pak.Extras...)
	writeBuf = append(writeBuf, pak.Key...)
	writeBuf = append(writeBuf, pak.Value...)

	// Write guarantees that err is returned if n<len, so we can just ignore
	// n and only inspect the error to determine if something went wrong...
	_, err := w.Write(writeBuf)
	if err != nil {
		return dispatchError{cause: err}
	}

	return nil
}
