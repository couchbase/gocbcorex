package gocbcorex

type DcpEvent interface {
	isDcpEvent() bool
}

type DcpSnapshotState uint32

const (
	DcpSnapshotStateInMemory         DcpSnapshotState = 1 << 0
	DcpSnapshotStateOnDisk           DcpSnapshotState = 1 << 1
	DcpSnapshotStateHistory          DcpSnapshotState = 1 << 4
	DcpSnapshotStateMayDuplicateKeys DcpSnapshotState = 1 << 5
)

type DcpSnapshotMarkerEvent struct {
	StartSeqNo         uint64
	EndSeqNo           uint64
	VbucketId          uint16
	SnapshotType       DcpSnapshotState
	MaxVisibleSeqNo    uint64
	HighCompletedSeqNo uint64
	SnapshotTimeStamp  uint64
}

func (DcpSnapshotMarkerEvent) isDcpEvent() bool { return true }

type DcpMutationEvent struct {
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
}

func (DcpMutationEvent) isDcpEvent() bool { return true }

type DcpDeletionEvent struct {
	SeqNo        uint64
	RevNo        uint64
	Cas          uint64
	DeleteTime   uint32
	CollectionId uint32
	VbucketId    uint16
	Datatype     uint8
	Key          []byte
	Value        []byte
}

func (DcpDeletionEvent) isDcpEvent() bool { return true }

type DcpExpirationEvent struct {
	SeqNo        uint64
	RevNo        uint64
	Cas          uint64
	DeleteTime   uint32
	CollectionId uint32
	VbucketId    uint16
	Key          []byte
}

func (DcpExpirationEvent) isDcpEvent() bool { return true }

type DcpCollectionCreationEvent struct {
	SeqNo        uint64
	Version      uint8
	VbucketId    uint16
	ManifestUid  uint64
	ScopeId      uint32
	CollectionId uint32
	Ttl          uint32
	Key          []byte
}

func (DcpCollectionCreationEvent) isDcpEvent() bool { return true }

type DcpCollectionDeletionEvent struct {
	SeqNo        uint64
	ManifestUid  uint64
	ScopeId      uint32
	CollectionId uint32
	VbucketId    uint16
	Version      uint8
}

func (DcpCollectionDeletionEvent) isDcpEvent() bool { return true }

type DcpCollectionFlushEvent struct {
	SeqNo        uint64
	Version      uint8
	VbucketId    uint16
	ManifestUid  uint64
	CollectionId uint32
}

func (DcpCollectionFlushEvent) isDcpEvent() bool { return true }

type DcpScopeCreationEvent struct {
	SeqNo       uint64
	Version     uint8
	VbucketId   uint16
	ManifestUid uint64
	ScopeId     uint32
	Key         []byte
}

func (DcpScopeCreationEvent) isDcpEvent() bool { return true }

type DcpScopeDeletionEvent struct {
	SeqNo       uint64
	Version     uint8
	VbucketId   uint16
	ManifestUid uint64
	ScopeId     uint32
}

func (DcpScopeDeletionEvent) isDcpEvent() bool { return true }

type DcpCollectionModificationEvent struct {
	SeqNo        uint64
	ManifestUid  uint64
	CollectionId uint32
	Ttl          uint32
	VbucketId    uint16
	Version      uint8
}

func (DcpCollectionModificationEvent) isDcpEvent() bool { return true }

type DcpOSOSnapshotEvent struct {
	SnapshotType uint32
	VbucketId    uint16
}

func (DcpOSOSnapshotEvent) isDcpEvent() bool { return true }

type DcpSeqNoAdvancedEvent struct {
	SeqNo     uint64
	VbucketId uint16
}

func (DcpSeqNoAdvancedEvent) isDcpEvent() bool { return true }

type DcpStreamEndEvent struct {
	VbucketId uint16
}

func (DcpStreamEndEvent) isDcpEvent() bool { return true }

type DcpEventsHandlers struct {
	DcpSnapshotMarker     func(req *DcpSnapshotMarkerEvent)
	DcpMutation           func(req *DcpMutationEvent)
	DcpDeletion           func(req *DcpDeletionEvent)
	DcpExpiration         func(req *DcpExpirationEvent)
	DcpCollectionCreation func(req *DcpCollectionCreationEvent)
	DcpCollectionDeletion func(req *DcpCollectionDeletionEvent)
	DcpCollectionFlush    func(req *DcpCollectionFlushEvent)
	DcpScopeCreation      func(req *DcpScopeCreationEvent)
	DcpScopeDeletion      func(req *DcpScopeDeletionEvent)
	DcpCollectionChanged  func(req *DcpCollectionModificationEvent)
	DcpStreamEnd          func(req *DcpStreamEndEvent)
	DcpOSOSnapshot        func(req *DcpOSOSnapshotEvent)
	DcpSeqNoAdvanced      func(req *DcpSeqNoAdvancedEvent)
}
