package memdx

import "encoding/hex"

// OpCode represents the specific command the packet is performing.
type ExtFrameCode uint16

const (
	ExtFrameCodeReqBarrier     = ExtFrameCode(0x00)
	ExtFrameCodeReqDurability  = ExtFrameCode(0x01)
	ExtFrameCodeReqStreamID    = ExtFrameCode(0x02)
	ExtFrameCodeReqOtelContext = ExtFrameCode(0x03)
	ExtFrameCodeReqOnBehalfOf  = ExtFrameCode(0x04)
	ExtFrameCodeReqPreserveTTL = ExtFrameCode(0x05)
	ExtFrameCodeReqExtraPerm   = ExtFrameCode(0x06)

	ExtFrameCodeResServerDuration   = ExtFrameCode(0x00)
	ExtFrameCodeResReadUnits        = ExtFrameCode(0x01)
	ExtFrameCodeResWriteUnits       = ExtFrameCode(0x02)
	ExtFrameCodeResThrottleDuration = ExtFrameCode(0x03)
)

func (c ExtFrameCode) String() string {
	reqStr := c.StringReqRes(true)
	resStr := c.StringReqRes(false)
	return reqStr + "/" + resStr
}

func (c ExtFrameCode) StringReqRes(isRequest bool) string {
	if isRequest {
		switch c {
		case ExtFrameCodeReqBarrier:
			return "ReqBarrier"
		case ExtFrameCodeReqDurability:
			return "ReqDurability"
		case ExtFrameCodeReqStreamID:
			return "ReqStreamID"
		case ExtFrameCodeReqOtelContext:
			return "ReqOtelContext"
		case ExtFrameCodeReqOnBehalfOf:
			return "ReqOnBehalfOf"
		case ExtFrameCodeReqPreserveTTL:
			return "ReqPreserveTTL"
		case ExtFrameCodeReqExtraPerm:
			return "ReqExtraPerm"
		}
	} else {
		switch c {
		case ExtFrameCodeResServerDuration:
			return "ResServerDuration"
		case ExtFrameCodeResReadUnits:
			return "ResReadUnits"
		case ExtFrameCodeResWriteUnits:
			return "ResWriteUnits"
		case ExtFrameCodeResThrottleDuration:
			return "ResThrottleDuration"
		}
	}

	return "x" + hex.EncodeToString([]byte{byte(c)})
}
