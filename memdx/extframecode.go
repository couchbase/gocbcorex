package memdx

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
