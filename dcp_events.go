package gocbcorex

type DcpEvent interface {
	IsDcpEvent() bool
}

type DcpSnapshotState uint32

const (
	DcpSnapshotStateInMemory         DcpSnapshotState = 1 << 0
	DcpSnapshotStateOnDisk           DcpSnapshotState = 1 << 1
	DcpSnapshotStateHistory          DcpSnapshotState = 1 << 4
	DcpSnapshotStateMayDuplicateKeys DcpSnapshotState = 1 << 5
)

type DcpSnapshotMarker struct {
	StartSeqNo         uint64
	EndSeqNo           uint64
	VbID               uint16
	StreamID           uint64
	SnapshotType       DcpSnapshotState
	MaxVisibleSeqNo    uint64
	HighCompletedSeqNo uint64
	SnapshotTimeStamp  uint64
}

func (DcpSnapshotMarker) IsDcpEvent() bool { return true }

type DcpMutation struct {
	SeqNo        uint64
	RevNo        uint64
	Cas          uint64
	Flags        uint32
	Expiry       uint32
	LockTime     uint32
	CollectionID uint32
	VbID         uint16
	StreamID     uint16
	Datatype     uint8
	Key          []byte
	Value        []byte
}

func (DcpMutation) IsDcpEvent() bool { return true }

type DcpDeletion struct {
	SeqNo        uint64
	RevNo        uint64
	Cas          uint64
	DeleteTime   uint32
	CollectionID uint32
	VbID         uint16
	StreamID     uint16
	Datatype     uint8
	Key          []byte
	Value        []byte
}

func (DcpDeletion) IsDcpEvent() bool { return true }

type DcpExpiration struct {
	SeqNo        uint64
	RevNo        uint64
	Cas          uint64
	DeleteTime   uint32
	CollectionID uint32
	VbID         uint16
	StreamID     uint16
	Key          []byte
}

func (DcpExpiration) IsDcpEvent() bool { return true }

type DcpCollectionCreation struct {
	SeqNo        uint64
	Version      uint8
	VbID         uint16
	ManifestUID  uint64
	ScopeID      uint32
	CollectionID uint32
	Ttl          uint32
	StreamID     uint16
	Key          []byte
}

func (DcpCollectionCreation) IsDcpEvent() bool { return true }

type DcpCollectionDeletion struct {
	SeqNo        uint64
	ManifestUID  uint64
	ScopeID      uint32
	CollectionID uint32
	StreamID     uint16
	VbID         uint16
	Version      uint8
}

func (DcpCollectionDeletion) IsDcpEvent() bool { return true }

type DcpCollectionFlush struct {
	SeqNo        uint64
	Version      uint8
	VbID         uint16
	ManifestUID  uint64
	CollectionID uint32
	StreamID     uint16
}

func (DcpCollectionFlush) IsDcpEvent() bool { return true }

type DcpScopeCreation struct {
	SeqNo       uint64
	Version     uint8
	VbID        uint16
	ManifestUID uint64
	ScopeID     uint32
	StreamID    uint16
	Key         []byte
}

func (DcpScopeCreation) IsDcpEvent() bool { return true }

type DcpScopeDeletion struct {
	SeqNo       uint64
	Version     uint8
	VbID        uint16
	ManifestUID uint64
	ScopeID     uint32
	StreamID    uint16
}

func (DcpScopeDeletion) IsDcpEvent() bool { return true }

type DcpCollectionModification struct {
	SeqNo        uint64
	ManifestUID  uint64
	CollectionID uint32
	Ttl          uint32
	VbID         uint16
	StreamID     uint16
	Version      uint8
}

func (DcpCollectionModification) IsDcpEvent() bool { return true }

type DcpOSOSnapshot struct {
	SnapshotType uint32
	VbID         uint16
	StreamID     uint16
}

func (DcpOSOSnapshot) IsDcpEvent() bool { return true }

type DcpSeqNoAdvanced struct {
	SeqNo    uint64
	VbID     uint16
	StreamID uint16
}

func (DcpSeqNoAdvanced) IsDcpEvent() bool { return true }
