package gocbcorex

import (
	"github.com/couchbase/gocbcorex/memdx"
)

type DcpStreamRouterStatic struct {
	ClientHandlers DcpEventsHandlers
}

func (r *DcpStreamRouterStatic) Handlers() DcpClientEventsHandlers {
	return DcpClientEventsHandlers{
		SnapshotMarker:     r.handleDcpSnapshotMarker,
		Mutation:           r.handleDcpMutation,
		Deletion:           r.handleDcpDeletion,
		Expiration:         r.handleDcpExpiration,
		CollectionCreation: r.handleDcpCollectionCreation,
		CollectionDeletion: r.handleDcpCollectionDeletion,
		CollectionFlush:    r.handleDcpCollectionFlush,
		ScopeCreation:      r.handleDcpScopeCreation,
		ScopeDeletion:      r.handleDcpScopeDeletion,
		CollectionChanged:  r.handleDcpCollectionChanged,
		StreamEnd:          r.handleDcpStreamEnd,
		OSOSnapshot:        r.handleDcpOSOSnapshot,
		SeqNoAdvanced:      r.handleDcpSeqNoAdvanced,
	}
}

func (c *DcpStreamRouterStatic) handleDcpSnapshotMarker(req *memdx.DcpSnapshotMarkerEvent) error {
	if c.ClientHandlers.SnapshotMarker != nil {
		c.ClientHandlers.SnapshotMarker(req)
	}
	return nil
}

func (c *DcpStreamRouterStatic) handleDcpMutation(req *memdx.DcpMutationEvent) error {
	if c.ClientHandlers.Mutation != nil {
		c.ClientHandlers.Mutation(req)
	}
	return nil
}

func (c *DcpStreamRouterStatic) handleDcpDeletion(req *memdx.DcpDeletionEvent) error {
	if c.ClientHandlers.Deletion != nil {
		c.ClientHandlers.Deletion(req)
	}
	return nil
}

func (c *DcpStreamRouterStatic) handleDcpExpiration(req *memdx.DcpExpirationEvent) error {
	if c.ClientHandlers.Expiration != nil {
		c.ClientHandlers.Expiration(req)
	}
	return nil
}

func (c *DcpStreamRouterStatic) handleDcpCollectionCreation(req *memdx.DcpCollectionCreationEvent) error {
	if c.ClientHandlers.CollectionCreation != nil {
		c.ClientHandlers.CollectionCreation(req)
	}
	return nil
}

func (c *DcpStreamRouterStatic) handleDcpCollectionDeletion(req *memdx.DcpCollectionDeletionEvent) error {
	if c.ClientHandlers.CollectionDeletion != nil {
		c.ClientHandlers.CollectionDeletion(req)
	}
	return nil
}

func (c *DcpStreamRouterStatic) handleDcpCollectionFlush(req *memdx.DcpCollectionFlushEvent) error {
	if c.ClientHandlers.CollectionFlush != nil {
		c.ClientHandlers.CollectionFlush(req)
	}
	return nil
}

func (c *DcpStreamRouterStatic) handleDcpScopeCreation(req *memdx.DcpScopeCreationEvent) error {
	if c.ClientHandlers.ScopeCreation != nil {
		c.ClientHandlers.ScopeCreation(req)
	}
	return nil
}

func (c *DcpStreamRouterStatic) handleDcpScopeDeletion(req *memdx.DcpScopeDeletionEvent) error {
	if c.ClientHandlers.ScopeDeletion != nil {
		c.ClientHandlers.ScopeDeletion(req)
	}
	return nil
}

func (c *DcpStreamRouterStatic) handleDcpCollectionChanged(req *memdx.DcpCollectionModificationEvent) error {
	if c.ClientHandlers.CollectionChanged != nil {
		c.ClientHandlers.CollectionChanged(req)
	}
	return nil
}

func (c *DcpStreamRouterStatic) handleDcpStreamEnd(req *memdx.DcpStreamEndEvent) error {
	if c.ClientHandlers.StreamEnd != nil {
		c.ClientHandlers.StreamEnd(req)
	}
	return nil
}

func (c *DcpStreamRouterStatic) handleDcpOSOSnapshot(req *memdx.DcpOSOSnapshotEvent) error {
	if c.ClientHandlers.OSOSnapshot != nil {
		c.ClientHandlers.OSOSnapshot(req)
	}
	return nil
}

func (c *DcpStreamRouterStatic) handleDcpSeqNoAdvanced(req *memdx.DcpSeqNoAdvancedEvent) error {
	if c.ClientHandlers.SeqNoAdvanced != nil {
		c.ClientHandlers.SeqNoAdvanced(req)
	}
	return nil
}
