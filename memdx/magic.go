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

	MagicSrvReq = Magic(0x82)
	MagicSrvRes = Magic(0x83)
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
	case MagicSrvReq:
		return "SrvReq"
	case MagicSrvRes:
		return "SrvRes"
	}

	return "x" + hex.EncodeToString([]byte{byte(m)})
}

func (m Magic) IsRequest() bool {
	return m == MagicReq || m == MagicReqExt || m == MagicSrvReq
}

func (m Magic) IsResponse() bool {
	return m == MagicRes || m == MagicResExt || m == MagicSrvRes
}

func (m Magic) IsExtended() bool {
	return m == MagicReqExt || m == MagicResExt
}

func (m Magic) IsServerInitiated() bool {
	return m == MagicSrvReq || m == MagicSrvRes
}
