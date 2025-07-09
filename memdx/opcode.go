package memdx

import "encoding/hex"

// OpCode represents the specific command the packet is performing.
// Note that this is not a 1:1 representation of the command on the
// wire as we segregate Cli and Srv commands using the first byte
// of this being a uint16
type OpCode uint16

const OpCodeTypeMask = 0xf000
const OpCodeTypeCli = 0x0000
const OpCodeTypeSrv = 0x1000

// These constants provide predefined values for all the operations
// which are supported by this library.
const (
	OpCodeGet                        = OpCode(OpCodeTypeCli) // | 0x00
	OpCodeSet                        = OpCode(OpCodeTypeCli | 0x01)
	OpCodeAdd                        = OpCode(OpCodeTypeCli | 0x02)
	OpCodeReplace                    = OpCode(OpCodeTypeCli | 0x03)
	OpCodeDelete                     = OpCode(OpCodeTypeCli | 0x04)
	OpCodeIncrement                  = OpCode(OpCodeTypeCli | 0x05)
	OpCodeDecrement                  = OpCode(OpCodeTypeCli | 0x06)
	OpCodeNoOp                       = OpCode(OpCodeTypeCli | 0x0a)
	OpCodeAppend                     = OpCode(OpCodeTypeCli | 0x0e)
	OpCodePrepend                    = OpCode(OpCodeTypeCli | 0x0f)
	OpCodeStat                       = OpCode(OpCodeTypeCli | 0x10)
	OpCodeTouch                      = OpCode(OpCodeTypeCli | 0x1c)
	OpCodeGAT                        = OpCode(OpCodeTypeCli | 0x1d)
	OpCodeHello                      = OpCode(OpCodeTypeCli | 0x1f)
	OpCodeSASLListMechs              = OpCode(OpCodeTypeCli | 0x20)
	OpCodeSASLAuth                   = OpCode(OpCodeTypeCli | 0x21)
	OpCodeSASLStep                   = OpCode(OpCodeTypeCli | 0x22)
	OpCodeGetAllVBSeqnos             = OpCode(OpCodeTypeCli | 0x48)
	OpCodeDcpOpenConnection          = OpCode(OpCodeTypeCli | 0x50)
	OpCodeDcpAddStream               = OpCode(OpCodeTypeCli | 0x51)
	OpCodeDcpCloseStream             = OpCode(OpCodeTypeCli | 0x52)
	OpCodeDcpStreamReq               = OpCode(OpCodeTypeCli | 0x53)
	OpCodeDcpGetFailoverLog          = OpCode(OpCodeTypeCli | 0x54)
	OpCodeDcpStreamEnd               = OpCode(OpCodeTypeCli | 0x55)
	OpCodeDcpSnapshotMarker          = OpCode(OpCodeTypeCli | 0x56)
	OpCodeDcpMutation                = OpCode(OpCodeTypeCli | 0x57)
	OpCodeDcpDeletion                = OpCode(OpCodeTypeCli | 0x58)
	OpCodeDcpExpiration              = OpCode(OpCodeTypeCli | 0x59)
	OpCodeDcpSeqNoAdvanced           = OpCode(OpCodeTypeCli | 0x64)
	OpCodeDcpOsoSnapshot             = OpCode(OpCodeTypeCli | 0x65)
	OpCodeDcpFlush                   = OpCode(OpCodeTypeCli | 0x5a)
	OpCodeDcpSetVbucketState         = OpCode(OpCodeTypeCli | 0x5b)
	OpCodeDcpNoOp                    = OpCode(OpCodeTypeCli | 0x5c)
	OpCodeDcpBufferAck               = OpCode(OpCodeTypeCli | 0x5d)
	OpCodeDcpControl                 = OpCode(OpCodeTypeCli | 0x5e)
	OpCodeDcpEvent                   = OpCode(OpCodeTypeCli | 0x5f)
	OpCodeGetReplica                 = OpCode(OpCodeTypeCli | 0x83)
	OpCodeSelectBucket               = OpCode(OpCodeTypeCli | 0x89)
	OpCodeObserveSeqNo               = OpCode(OpCodeTypeCli | 0x91)
	OpCodeObserve                    = OpCode(OpCodeTypeCli | 0x92)
	OpCodeGetLocked                  = OpCode(OpCodeTypeCli | 0x94)
	OpCodeUnlockKey                  = OpCode(OpCodeTypeCli | 0x95)
	OpCodeGetMeta                    = OpCode(OpCodeTypeCli | 0xa0)
	OpCodeSetWithMeta                = OpCode(OpCodeTypeCli | 0xa2)
	OpCodeAddWithMeta                = OpCode(OpCodeTypeCli | 0xa4)
	OpCodeDelWithMeta                = OpCode(OpCodeTypeCli | 0xa8)
	OpCodeGetClusterConfig           = OpCode(OpCodeTypeCli | 0xb5)
	OpCodeGetRandom                  = OpCode(OpCodeTypeCli | 0xb6)
	OpCodeCollectionsGetManifest     = OpCode(OpCodeTypeCli | 0xba)
	OpCodeCollectionsGetID           = OpCode(OpCodeTypeCli | 0xbb)
	OpCodeSubDocGet                  = OpCode(OpCodeTypeCli | 0xc5)
	OpCodeSubDocExists               = OpCode(OpCodeTypeCli | 0xc6)
	OpCodeSubDocDictAdd              = OpCode(OpCodeTypeCli | 0xc7)
	OpCodeSubDocDictSet              = OpCode(OpCodeTypeCli | 0xc8)
	OpCodeSubDocDelete               = OpCode(OpCodeTypeCli | 0xc9)
	OpCodeSubDocReplace              = OpCode(OpCodeTypeCli | 0xca)
	OpCodeSubDocArrayPushLast        = OpCode(OpCodeTypeCli | 0xcb)
	OpCodeSubDocArrayPushFirst       = OpCode(OpCodeTypeCli | 0xcc)
	OpCodeSubDocArrayInsert          = OpCode(OpCodeTypeCli | 0xcd)
	OpCodeSubDocArrayAddUnique       = OpCode(OpCodeTypeCli | 0xce)
	OpCodeSubDocCounter              = OpCode(OpCodeTypeCli | 0xcf)
	OpCodeSubDocMultiLookup          = OpCode(OpCodeTypeCli | 0xd0)
	OpCodeSubDocMultiMutation        = OpCode(OpCodeTypeCli | 0xd1)
	OpCodeSubDocGetCount             = OpCode(OpCodeTypeCli | 0xd2)
	OpCodeSubDocReplaceBodyWithXattr = OpCode(OpCodeTypeCli | 0xd3)
	OpCodeRangeScanCreate            = OpCode(OpCodeTypeCli | 0xda)
	OpCodeRangeScanContinue          = OpCode(OpCodeTypeCli | 0xdb)
	OpCodeRangeScanCancel            = OpCode(OpCodeTypeCli | 0xdc)
	OpCodeGetErrorMap                = OpCode(OpCodeTypeCli | 0xfe)

	OpCodeSrvClustermapChangeNotification = OpCode(OpCodeTypeSrv | 0x01)
	OpCodeSrvAuthenticate                 = OpCode(OpCodeTypeSrv | 0x02)
	OpCodeSrvActiveExternalUsers          = OpCode(OpCodeTypeSrv | 0x03)
)

func (c OpCode) IsCli() bool {
	return c&OpCodeTypeMask == OpCodeTypeCli
}
func (c OpCode) IsSrv() bool {
	return c&OpCodeTypeMask == OpCodeTypeSrv
}

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
	case OpCodeSetWithMeta:
		return "SetWithMeta"
	case OpCodeAddWithMeta:
		return "AddWithMeta"
	case OpCodeDelWithMeta:
		return "DelWithMeta"
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

	case OpCodeSrvClustermapChangeNotification:
		return "SrvClustermapChangeNotification"
	case OpCodeSrvAuthenticate:
		return "SrvAuthenticate"
	case OpCodeSrvActiveExternalUsers:
		return "SrvActiveExternalUsers"
	}

	switch c & OpCodeTypeMask {
	case OpCodeTypeCli:
		return "x" + hex.EncodeToString([]byte{byte(c)}) + "[Cli]"
	case OpCodeTypeSrv:
		return "x" + hex.EncodeToString([]byte{byte(c)}) + "[Srv]"
	default:
		return "x" + hex.EncodeToString([]byte{byte(c << 8), byte(c)}) + "[Unk]"
	}
}
