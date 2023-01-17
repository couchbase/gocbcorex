package memdx

import "encoding/hex"

type Magic uint8

const (
	// CmdMagicReq indicates that the packet is a request.
	MagicReq = Magic(0x80)

	// CmdMagicRes indicates that the packet is a response.
	MagicRes = Magic(0x81)

	// MagicReqExt indicates that the packet is a request with framing extras.
	MagicReqExt = Magic(0x08)

	// MagicResExt indicates that the packet is a response with framing extras.
	MagicResExt = Magic(0x18)
)

func (m Magic) String() string {
	switch m {
	case MagicReq:
		return "Req"
	case MagicRes:
		return "Res"
	case MagicReqExt:
		return "ReqExt"
	case MagicResExt:
		return "ResExt"
	}

	return "x" + hex.EncodeToString([]byte{byte(m)})
}

func (m Magic) IsRequest() bool {
	return m == MagicReq || m == MagicReqExt
}

func (m Magic) IsResponse() bool {
	return m == MagicRes || m == MagicResExt
}

func (m Magic) IsExtended() bool {
	return m == MagicReqExt || m == MagicResExt
}
