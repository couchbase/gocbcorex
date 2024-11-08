package gocbcorex

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/couchbase/gocbcorex/memdx"
)

type dcpStreamRouterDynState struct {
	streams map[uint16]DcpEventsHandlers
}

type DcpStreamRouterDyn struct {
	lock    sync.Mutex
	streams map[uint16]DcpEventsHandlers

	state atomic.Pointer[dcpStreamRouterDynState]
}

func (r *DcpStreamRouterDyn) Register(streamId uint16, handlers DcpEventsHandlers) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.streams == nil {
		r.streams = make(map[uint16]DcpEventsHandlers)
	}

	if _, ok := r.streams[streamId]; ok {
		return errors.New("cannot register duplicate dcp stream")
	}

	r.streams[streamId] = handlers
	r.rebuildStateLocked()

	return nil
}

func (r *DcpStreamRouterDyn) Unregister(streamId uint16) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.streams == nil {
		return
	}

	delete(r.streams, streamId)
	r.rebuildStateLocked()
}

func (r *DcpStreamRouterDyn) rebuildStateLocked() {
	state := &dcpStreamRouterDynState{
		streams: make(map[uint16]DcpEventsHandlers, len(r.streams)),
	}
	if r.streams != nil {
		for streamKey, handlers := range r.streams {
			state.streams[streamKey] = handlers
		}
	}
	r.state.Store(state)
}

func (r *DcpStreamRouterDyn) getHandlers(streamId uint16) (DcpEventsHandlers, bool) {
	state := r.state.Load()
	handlers, ok := state.streams[streamId]
	return handlers, ok
}

func (r *DcpStreamRouterDyn) Handlers() DcpClientEventsHandlers {
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

func (c *DcpStreamRouterDyn) handleDcpSnapshotMarker(req *memdx.DcpSnapshotMarkerEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.SnapshotMarker != nil {
		handlers.SnapshotMarker(req)
	}
	return nil
}

func (c *DcpStreamRouterDyn) handleDcpMutation(req *memdx.DcpMutationEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.Mutation != nil {
		handlers.Mutation(req)
	}
	return nil
}

func (c *DcpStreamRouterDyn) handleDcpDeletion(req *memdx.DcpDeletionEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.Deletion != nil {
		handlers.Deletion(req)
	}
	return nil
}

func (c *DcpStreamRouterDyn) handleDcpExpiration(req *memdx.DcpExpirationEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.Expiration != nil {
		handlers.Expiration(req)
	}
	return nil
}

func (c *DcpStreamRouterDyn) handleDcpCollectionCreation(req *memdx.DcpCollectionCreationEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.CollectionCreation != nil {
		handlers.CollectionCreation(req)
	}
	return nil
}

func (c *DcpStreamRouterDyn) handleDcpCollectionDeletion(req *memdx.DcpCollectionDeletionEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.CollectionDeletion != nil {
		handlers.CollectionDeletion(req)
	}
	return nil
}

func (c *DcpStreamRouterDyn) handleDcpCollectionFlush(req *memdx.DcpCollectionFlushEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.CollectionFlush != nil {
		handlers.CollectionFlush(req)
	}
	return nil
}

func (c *DcpStreamRouterDyn) handleDcpScopeCreation(req *memdx.DcpScopeCreationEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.ScopeCreation != nil {
		handlers.ScopeCreation(req)
	}
	return nil
}

func (c *DcpStreamRouterDyn) handleDcpScopeDeletion(req *memdx.DcpScopeDeletionEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.ScopeDeletion != nil {
		handlers.ScopeDeletion(req)
	}
	return nil
}

func (c *DcpStreamRouterDyn) handleDcpCollectionChanged(req *memdx.DcpCollectionModificationEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.CollectionChanged != nil {
		handlers.CollectionChanged(req)
	}
	return nil
}

func (c *DcpStreamRouterDyn) handleDcpStreamEnd(req *memdx.DcpStreamEndEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.StreamEnd != nil {
		handlers.StreamEnd(req)
	}
	return nil
}

func (c *DcpStreamRouterDyn) handleDcpOSOSnapshot(req *memdx.DcpOSOSnapshotEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.OSOSnapshot != nil {
		handlers.OSOSnapshot(req)
	}
	return nil
}

func (c *DcpStreamRouterDyn) handleDcpSeqNoAdvanced(req *memdx.DcpSeqNoAdvancedEvent) error {
	if handlers, ok := c.getHandlers(req.StreamId); ok && handlers.SeqNoAdvanced != nil {
		handlers.SeqNoAdvanced(req)
	}
	return nil
}
