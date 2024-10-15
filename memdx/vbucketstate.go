package memdx

type VbucketState uint32

const (
	VbucketStateActive  = VbucketState(0x01)
	VbucketStateReplica = VbucketState(0x02)
	VbucketStatePending = VbucketState(0x03)
	VbucketStateDead    = VbucketState(0x04)
)
