package memdx

import "encoding/hex"

// OpCode represents the specific command the packet is performing.
type OpCode uint8

// These constants provide predefined values for all the operations
// which are supported by this library.
const (
	OpCodeGet                        = OpCode(0x00)
	OpCodeSet                        = OpCode(0x01)
	OpCodeAdd                        = OpCode(0x02)
	OpCodeReplace                    = OpCode(0x03)
	OpCodeDelete                     = OpCode(0x04)
	OpCodeIncrement                  = OpCode(0x05)
	OpCodeDecrement                  = OpCode(0x06)
	OpCodeNoOp                       = OpCode(0x0a)
	OpCodeAppend                     = OpCode(0x0e)
	OpCodePrepend                    = OpCode(0x0f)
	OpCodeStat                       = OpCode(0x10)
	OpCodeTouch                      = OpCode(0x1c)
	OpCodeGAT                        = OpCode(0x1d)
	OpCodeHello                      = OpCode(0x1f)
	OpCodeSASLListMechs              = OpCode(0x20)
	OpCodeSASLAuth                   = OpCode(0x21)
	OpCodeSASLStep                   = OpCode(0x22)
	OpCodeGetAllVBSeqnos             = OpCode(0x48)
	OpCodeDcpOpenConnection          = OpCode(0x50)
	OpCodeDcpAddStream               = OpCode(0x51)
	OpCodeDcpCloseStream             = OpCode(0x52)
	OpCodeDcpStreamReq               = OpCode(0x53)
	OpCodeDcpGetFailoverLog          = OpCode(0x54)
	OpCodeDcpStreamEnd               = OpCode(0x55)
	OpCodeDcpSnapshotMarker          = OpCode(0x56)
	OpCodeDcpMutation                = OpCode(0x57)
	OpCodeDcpDeletion                = OpCode(0x58)
	OpCodeDcpExpiration              = OpCode(0x59)
	OpCodeDcpSeqNoAdvanced           = OpCode(0x64)
	OpCodeDcpOsoSnapshot             = OpCode(0x65)
	OpCodeDcpFlush                   = OpCode(0x5a)
	OpCodeDcpSetVbucketState         = OpCode(0x5b)
	OpCodeDcpNoOp                    = OpCode(0x5c)
	OpCodeDcpBufferAck               = OpCode(0x5d)
	OpCodeDcpControl                 = OpCode(0x5e)
	OpCodeDcpEvent                   = OpCode(0x5f)
	OpCodeGetReplica                 = OpCode(0x83)
	OpCodeSelectBucket               = OpCode(0x89)
	OpCodeObserveSeqNo               = OpCode(0x91)
	OpCodeObserve                    = OpCode(0x92)
	OpCodeGetLocked                  = OpCode(0x94)
	OpCodeUnlockKey                  = OpCode(0x95)
	OpCodeGetMeta                    = OpCode(0xa0)
	OpCodeSetMeta                    = OpCode(0xa2)
	OpCodeDelMeta                    = OpCode(0xa8)
	OpCodeGetClusterConfig           = OpCode(0xb5)
	OpCodeGetRandom                  = OpCode(0xb6)
	OpCodeCollectionsGetManifest     = OpCode(0xba)
	OpCodeCollectionsGetID           = OpCode(0xbb)
	OpCodeSubDocGet                  = OpCode(0xc5)
	OpCodeSubDocExists               = OpCode(0xc6)
	OpCodeSubDocDictAdd              = OpCode(0xc7)
	OpCodeSubDocDictSet              = OpCode(0xc8)
	OpCodeSubDocDelete               = OpCode(0xc9)
	OpCodeSubDocReplace              = OpCode(0xca)
	OpCodeSubDocArrayPushLast        = OpCode(0xcb)
	OpCodeSubDocArrayPushFirst       = OpCode(0xcc)
	OpCodeSubDocArrayInsert          = OpCode(0xcd)
	OpCodeSubDocArrayAddUnique       = OpCode(0xce)
	OpCodeSubDocCounter              = OpCode(0xcf)
	OpCodeSubDocMultiLookup          = OpCode(0xd0)
	OpCodeSubDocMultiMutation        = OpCode(0xd1)
	OpCodeSubDocGetCount             = OpCode(0xd2)
	OpCodeSubDocReplaceBodyWithXattr = OpCode(0xd3)
	OpCodeRangeScanCreate            = OpCode(0xda)
	OpCodeRangeScanContinue          = OpCode(0xdb)
	OpCodeRangeScanCancel            = OpCode(0xdc)
	OpCodeGetErrorMap                = OpCode(0xfe)
)

// String returns the string representation of the OpCode.
func (c OpCode) String() string {
	switch c {
	case OpCodeGet:
		return "Get"
	case OpCodeSet:
		return "Set"
	case OpCodeAdd:
		return "Add"
	case OpCodeReplace:
		return "Replace"
	case OpCodeDelete:
		return "Delete"
	case OpCodeIncrement:
		return "Increment"
	case OpCodeDecrement:
		return "Decrement"
	case OpCodeNoOp:
		return "Noop"
	case OpCodeAppend:
		return "Append"
	case OpCodePrepend:
		return "Prepend"
	case OpCodeStat:
		return "Stat"
	case OpCodeTouch:
		return "Touch"
	case OpCodeGAT:
		return "GAT"
	case OpCodeHello:
		return "Hello"
	case OpCodeSASLListMechs:
		return "SASLListMechs"
	case OpCodeSASLAuth:
		return "SASLAuth"
	case OpCodeSASLStep:
		return "SASLStep"
	case OpCodeGetAllVBSeqnos:
		return "GetAllVBSeqnos"
	case OpCodeDcpOpenConnection:
		return "DcpOpenConnection"
	case OpCodeDcpAddStream:
		return "DcpAddStream"
	case OpCodeDcpCloseStream:
		return "DcpCloseStream"
	case OpCodeDcpStreamReq:
		return "DcpStreamReq"
	case OpCodeDcpGetFailoverLog:
		return "DcpGetFailoverLog"
	case OpCodeDcpStreamEnd:
		return "DcpStreamEnd"
	case OpCodeDcpSnapshotMarker:
		return "DcpSnapshotMarker"
	case OpCodeDcpMutation:
		return "DcpMutation"
	case OpCodeDcpDeletion:
		return "DcpDeletion"
	case OpCodeDcpExpiration:
		return "DcpExpiration"
	case OpCodeDcpFlush:
		return "DcpFlush"
	case OpCodeDcpSetVbucketState:
		return "DcpSetVbucketState"
	case OpCodeDcpNoOp:
		return "DcpNoOp"
	case OpCodeDcpBufferAck:
		return "DcpBufferAck"
	case OpCodeDcpControl:
		return "DcpControl"
	case OpCodeGetReplica:
		return "GetReplica"
	case OpCodeSelectBucket:
		return "SelectBucket"
	case OpCodeObserveSeqNo:
		return "ObserveSeqNo"
	case OpCodeObserve:
		return "Observe"
	case OpCodeGetLocked:
		return "GetLocked"
	case OpCodeUnlockKey:
		return "UnlockKey"
	case OpCodeGetMeta:
		return "GetMeta"
	case OpCodeSetMeta:
		return "SetMeta"
	case OpCodeDelMeta:
		return "DelMeta"
	case OpCodeGetClusterConfig:
		return "GetClusterConfig"
	case OpCodeGetRandom:
		return "GetRandom"
	case OpCodeSubDocGet:
		return "SubDocGet"
	case OpCodeSubDocExists:
		return "SubDocExists"
	case OpCodeSubDocDictAdd:
		return "SubDocDictAdd"
	case OpCodeSubDocDictSet:
		return "SubDocDictSet"
	case OpCodeSubDocDelete:
		return "SubDocDelete"
	case OpCodeSubDocReplace:
		return "SubDocReplace"
	case OpCodeSubDocArrayPushLast:
		return "SubDocArrayPushLast"
	case OpCodeSubDocArrayPushFirst:
		return "SubDocArrayPushFirst"
	case OpCodeSubDocArrayInsert:
		return "SubDocArrayInsert"
	case OpCodeSubDocArrayAddUnique:
		return "SubDocArrayAddUnique"
	case OpCodeSubDocCounter:
		return "SubDocCounter"
	case OpCodeSubDocMultiLookup:
		return "SubDocMultiLookup"
	case OpCodeSubDocMultiMutation:
		return "SubDocMultiMutation"
	case OpCodeSubDocGetCount:
		return "SubDocGetCount"
	case OpCodeGetErrorMap:
		return "GetErrorMap"
	case OpCodeCollectionsGetID:
		return "CollectionsGetID"
	case OpCodeCollectionsGetManifest:
		return "CollectionsGetManifest"
	}

	return "x" + hex.EncodeToString([]byte{byte(c)})
}
