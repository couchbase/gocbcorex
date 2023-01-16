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
	OpCodeNoop                       = OpCode(0x0a)
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
	OpCodeDcpNoop                    = OpCode(0x5c)
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
	OpCodeGetErrorMap                = OpCode(0xfe)
)

// Name returns the string representation of the OpCode.
func (command OpCode) Name() string {
	switch command {
	case OpCodeGet:
		return "GET"
	case OpCodeSet:
		return "SET"
	case OpCodeAdd:
		return "ADD"
	case OpCodeReplace:
		return "REPLACE"
	case OpCodeDelete:
		return "DELETE"
	case OpCodeIncrement:
		return "INCREMENT"
	case OpCodeDecrement:
		return "DECREMENT"
	case OpCodeNoop:
		return "NOOP"
	case OpCodeAppend:
		return "APPEND"
	case OpCodePrepend:
		return "PREPEND"
	case OpCodeStat:
		return "STAT"
	case OpCodeTouch:
		return "TOUCH"
	case OpCodeGAT:
		return "GAT"
	case OpCodeHello:
		return "HELLO"
	case OpCodeSASLListMechs:
		return "SASLLISTMECHS"
	case OpCodeSASLAuth:
		return "SASLAUTH"
	case OpCodeSASLStep:
		return "SASLSTEP"
	case OpCodeGetAllVBSeqnos:
		return "GETALLVBSEQNOS"
	case OpCodeDcpOpenConnection:
		return "DCPOPENCONNECTION"
	case OpCodeDcpAddStream:
		return "DCPADDSTREAM"
	case OpCodeDcpCloseStream:
		return "DCPCLOSESTREAM"
	case OpCodeDcpStreamReq:
		return "DCPSTREAMREQ"
	case OpCodeDcpGetFailoverLog:
		return "DCPGETFAILOVERLOG"
	case OpCodeDcpStreamEnd:
		return "DCPSTREAMEND"
	case OpCodeDcpSnapshotMarker:
		return "DCPSNAPSHOTMARKER"
	case OpCodeDcpMutation:
		return "DCPMUTATION"
	case OpCodeDcpDeletion:
		return "DCPDELETION"
	case OpCodeDcpExpiration:
		return "DCPEXPIRATION"
	case OpCodeDcpFlush:
		return "DCPFLUSH"
	case OpCodeDcpSetVbucketState:
		return "DCPSETVBUCKETSTATE"
	case OpCodeDcpNoop:
		return "DCPNOOP"
	case OpCodeDcpBufferAck:
		return "DCPBUFFERACK"
	case OpCodeDcpControl:
		return "DCPCONTROL"
	case OpCodeGetReplica:
		return "GETREPLICA"
	case OpCodeSelectBucket:
		return "SELECTBUCKET"
	case OpCodeObserveSeqNo:
		return "OBSERVESEQNO"
	case OpCodeObserve:
		return "OBSERVE"
	case OpCodeGetLocked:
		return "GET_LOCKED"
	case OpCodeUnlockKey:
		return "UNLOCK"
	case OpCodeGetMeta:
		return "GET_META"
	case OpCodeSetMeta:
		return "SET_META"
	case OpCodeDelMeta:
		return "DEL_META"
	case OpCodeGetClusterConfig:
		return "GETCLUSTERCONFIG"
	case OpCodeGetRandom:
		return "GETRANDOM"
	case OpCodeSubDocGet:
		return "SUBDOCGET"
	case OpCodeSubDocExists:
		return "SUBDOCEXISTS"
	case OpCodeSubDocDictAdd:
		return "SUBDOCDICTADD"
	case OpCodeSubDocDictSet:
		return "SUBDOCDICTSET"
	case OpCodeSubDocDelete:
		return "SUBDOCDELETE"
	case OpCodeSubDocReplace:
		return "SUBDOCREPLACE"
	case OpCodeSubDocArrayPushLast:
		return "SUBDOCARRAYPUSHLAST"
	case OpCodeSubDocArrayPushFirst:
		return "SUBDOCARRAYPUSHFIRST"
	case OpCodeSubDocArrayInsert:
		return "SUBDOCARRAYINSERT"
	case OpCodeSubDocArrayAddUnique:
		return "SUBDOCARRAYADDUNIQUE"
	case OpCodeSubDocCounter:
		return "SUBDOCCOUNTER"
	case OpCodeSubDocMultiLookup:
		return "SUBDOCMULTILOOKUP"
	case OpCodeSubDocMultiMutation:
		return "SUBDOCMULTIMUTATION"
	case OpCodeSubDocGetCount:
		return "SUBDOCGETCOUNT"
	case OpCodeGetErrorMap:
		return "GETERRORMAP"
	case OpCodeCollectionsGetID:
		return "GETCOLLECTIONID"
	case OpCodeCollectionsGetManifest:
		return "GETCOLLECTIONMANIFEST"
	default:
		return "x" + hex.EncodeToString([]byte{byte(command)})
	}
}
