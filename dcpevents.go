package gocbcorex

import "github.com/couchbase/gocbcorex/memdx"

type DcpEventsHandlers struct {
	StreamOpen         func(req *memdx.DcpStreamReqResponse)
	StreamEnd          func(req *memdx.DcpStreamEndEvent)
	SnapshotMarker     func(req *memdx.DcpSnapshotMarkerEvent)
	Mutation           func(req *memdx.DcpMutationEvent)
	Deletion           func(req *memdx.DcpDeletionEvent)
	Expiration         func(req *memdx.DcpExpirationEvent)
	CollectionCreation func(req *memdx.DcpCollectionCreationEvent)
	CollectionDeletion func(req *memdx.DcpCollectionDeletionEvent)
	CollectionFlush    func(req *memdx.DcpCollectionFlushEvent)
	ScopeCreation      func(req *memdx.DcpScopeCreationEvent)
	ScopeDeletion      func(req *memdx.DcpScopeDeletionEvent)
	CollectionChanged  func(req *memdx.DcpCollectionModificationEvent)
	OSOSnapshot        func(req *memdx.DcpOSOSnapshotEvent)
	SeqNoAdvanced      func(req *memdx.DcpSeqNoAdvancedEvent)
}
