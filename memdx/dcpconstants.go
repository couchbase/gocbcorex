package memdx

type DcpConnectionFlags uint32

const (
	DcpConnectionFlagsProducer           = 1 << 0
	DcpConnectionFlagsIncludeXattrs      = 1 << 2
	DcpConnectionFlagsNoValue            = 1 << 3
	DcpConnectionFlagsIncludeDeleteTime  = 1 << 5
	DcpConnectionFlagsUnderlyingDatatype = 1 << 6
	DcpConnectionFlagsDeleteUserXattrs   = 1 << 8
)

type DcpStreamEventCode uint32

const (
	DcpStreamEventCodeCollectionCreate  DcpStreamEventCode = 0x00
	DcpStreamEventCodeCollectionDelete  DcpStreamEventCode = 0x01
	DcpStreamEventCodeCollectionFlush   DcpStreamEventCode = 0x02
	DcpStreamEventCodeScopeCreate       DcpStreamEventCode = 0x03
	DcpStreamEventCodeScopeDelete       DcpStreamEventCode = 0x04
	DcpStreamEventCodeCollectionChanged DcpStreamEventCode = 0x05
)

type VbucketState uint32

const (
	VbucketStateActive  = VbucketState(0x01)
	VbucketStateReplica = VbucketState(0x02)
	VbucketStatePending = VbucketState(0x03)
	VbucketStateDead    = VbucketState(0x04)
)
