package memdx

import (
	"encoding/binary"
	"io"
)

type PacketReader struct {
	// we use this heap-allocated read buffer since io.Read will cause
	// the buffer to escape.  the payload portion of the packet is
	// allocated on-demand since it will _always_ escape through references
	// that existing in the *Packet object.
	readHeaderBuf []byte
}

func (pr *PacketReader) ReadPacket(r io.Reader, pak *Packet) error {
	if len(pr.readHeaderBuf) != 24 {
		pr.readHeaderBuf = make([]byte, 24)
	}
	headerBuf := pr.readHeaderBuf

	_, err := io.ReadFull(r, headerBuf)
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
	_, err = io.ReadFull(r, payloadBuf)
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
