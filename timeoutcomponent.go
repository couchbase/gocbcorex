package core

import (
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"time"
)

type finalCallback func(ctx *asyncContext, pak *memd.Packet, client interface{}, err error) bool

type TimeoutComponent struct {
}

func (tc *TimeoutComponent) Dispatch(ctx *asyncContext, deadline time.Time, dispatchCb func(*asyncContext, finalCallback), finalCb finalCallback) {
	if deadline.IsZero() {
		dispatchCb(ctx, finalCb)
		return
	}

	timer := time.AfterFunc(time.Until(deadline), func() {
		// TODO: We need to know if this is ambiguous or not.
		ctx.InternalCancel(gocbcore.ErrTimeout)
	})

	newFinalCb := func(ctx *asyncContext, pak *memd.Packet, client interface{}, err error) bool {
		// We could get here because the timer fired, but it's ok to call stop anyway.
		timer.Stop()
		return finalCb(ctx, pak, client, err)
	}

	dispatchCb(ctx, newFinalCb)
}

func (o *CrudOperator) Get(opts GetOptions) {
	ctx := &asyncContext{}

	a.timeouts.Dispatch(ctx, time.Now(), func(ctx2 *asyncContext, prev finalCallback) {
		o.collectionResolver.ResolveCollectionID(
			ctx,
			opts.ScopeName,
			opts.CollectionName,
			func(collectionId uint32, manifestRev uint64, err error) {
				pak := &memd.Packet{
					// ...
				}
				o.vbucketDispatcher.DispatchToVbucket(
					ctx,
					pak.Vbucket,
					pak,
					func(resp *memd.Packet, client KvClient, err error) bool {
						// err - only _ACTUAL_ errors
						// resp - may contain non-success status codes...

						// ...
						return false
					})
			}, prev)
	}, func(ctx *asyncContext, pak *memd.Packet, client interface{}, err error) bool {

		cb()
		return false
	})

	// What do I return!?
}
