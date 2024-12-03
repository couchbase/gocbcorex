package memdx

type ClustermapChangeEvent struct {
	BucketName []byte
	RevEpoch   int64
	Rev        int64
	Config     []byte
}

type DcpSnapshotState uint32

const (
	DcpSnapshotStateInMemory         DcpSnapshotState = 1 << 0
	DcpSnapshotStateOnDisk           DcpSnapshotState = 1 << 1
	DcpSnapshotStateHistory          DcpSnapshotState = 1 << 4
	DcpSnapshotStateMayDuplicateKeys DcpSnapshotState = 1 << 5
)

type DcpSnapshotMarkerEvent struct {
	StreamId           uint16
	Version            int
	StartSeqNo         uint64
	EndSeqNo           uint64
	VbucketId          uint16
	SnapshotType       DcpSnapshotState
	MaxVisibleSeqNo    uint64
	HighCompletedSeqNo uint64
	SnapshotTimeStamp  uint64
}

type DcpMutationEvent struct {
	StreamId     uint16
	Version      int
	SeqNo        uint64
	RevNo        uint64
	Cas          uint64
	Flags        uint32
	Expiry       uint32
	LockTime     uint32
	CollectionId uint32
	VbucketId    uint16
	Datatype     uint8
	Key          []byte
	Value        []byte
	MetaDataSize uint16
	NRU          uint8
}

type DcpDeletionEvent struct {
	StreamId     uint16
	Version      int
	SeqNo        uint64
	RevNo        uint64
	Cas          uint64
	DeleteTime   uint32
	CollectionId uint32
	VbucketId    uint16
	Datatype     uint8
	Key          []byte
}

type DcpExpirationEvent struct {
	StreamId     uint16
	Version      int
	SeqNo        uint64
	RevNo        uint64
	Cas          uint64
	DeleteTime   uint32
	CollectionId uint32
	VbucketId    uint16
	Datatype     uint8
	Key          []byte
}

type DcpCollectionCreationEvent struct {
	StreamId       uint16
	SeqNo          uint64
	Version        uint8
	VbucketId      uint16
	ManifestUid    uint64
	ScopeId        uint32
	CollectionId   uint32
	Ttl            uint32
	CollectionName string
}

type DcpCollectionDeletionEvent struct {
	StreamId     uint16
	Version      uint8
	SeqNo        uint64
	ManifestUid  uint64
	ScopeID      uint32
	CollectionId uint32
	VbucketId    uint16
}

type DcpCollectionFlushEvent struct {
	StreamId     uint16
	Version      uint8
	SeqNo        uint64
	VbucketId    uint16
	ManifestUid  uint64
	CollectionId uint32
}

type DcpScopeCreationEvent struct {
	StreamId    uint16
	Version     uint8
	SeqNo       uint64
	VbucketId   uint16
	ManifestUid uint64
	ScopeId     uint32
	ScopeName   string
}

type DcpScopeDeletionEvent struct {
	StreamId    uint16
	Version     uint8
	SeqNo       uint64
	VbucketId   uint16
	ManifestUid uint64
	ScopeId     uint32
}

type DcpCollectionModificationEvent struct {
	StreamId     uint16
	Version      uint8
	SeqNo        uint64
	ManifestUid  uint64
	CollectionId uint32
	Ttl          uint32
	VbucketId    uint16
}

type DcpStreamEndEvent struct {
	StreamId  uint16
	VbucketId uint16
}

type DcpOSOSnapshotEvent struct {
	StreamId     uint16
	SnapshotType uint32
	VbucketId    uint16
}

type DcpSeqNoAdvancedEvent struct {
	StreamId  uint16
	SeqNo     uint64
	VbucketId uint16
}

type DcpNoOpEvent struct {
}

type DcpNoOpEventResponse struct {
}
