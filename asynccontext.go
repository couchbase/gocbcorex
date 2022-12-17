package core

import (
	"context"
	"sync"
)

/*
There are three methods that can be invoked in various orders.
  - Cancel()
  - MarkComplete()
  - OnCancel()

The combinations look like:
  - Cancel -> MarkComplete -> OnCancel
	  In this instance, MarkComplete will return false to indicate it was
	  cancelled.  OnCancel will immediately invoke the cancel callback.
  - Cancel -> OnCancel -> MarkComplete
      In this case, OnCancel will immediately invoke the cancel callback.
	  If it returns true, indicating successful cancellation, MarkComplete
	  must never be called.  The AsyncCancelContext is no longer valid.
	  If it returns false, indicating failed cancellation, MarkComplete is
	  expected to be called shortly.  The AsyncCancelContext remains
	  valid until MarkComplete is invoked.
  - MarkComplete -> Cancel -> OnCancel
      In this case, MarkComplete will return true and the users callback
	  will be invoked.  MarkComplete means the users callback might be
	  invoked, which could free the AsyncContext.  This means that in
	  this case MarkComplete must ensure OnCancel doesn't rely on the
	  state of the AsyncContext anymore.
  - OnCancel -> Cancel -> MarkComplete
      OnCancel will register the handler.  Cancel is then invoked which will
	  invoke the cancel handler.
	  If it returns true, indicating successful cancellation, MarkComplete
	  must never be called.  The AsyncCancelContext is no longer valid.
	  If it returns false, indicating failed cancellation, MarkComplete is
	  expected to be called shortly.  The AsyncCancelContext remains
	  valid until MarkComplete is invoked.
  - MarkComplete -> OnCancel -> Cancel
      See above case of [MarkComplete -> Cancel -> OnCancel]
  - OnCancel -> MarkComplete -> Cancel
      OnCancel will register the handler.  MarkComplete will then remove
	  the handler and deregister this AsyncCancelContext from the
	  AsyncContext. Cancel() has no effect.
*/

/*
AsyncContext encapsulates much of the logic of a context.Context but aligns
it with the callback-oriented nature of this library.
*/
type AsyncContext struct {
	// this provides concurrency-safety for all the fields in this AsyncContext.
	// this is considered performant as sync.Mutex is implemented as a CAS when
	// the mutex is uncontended, and the only contention that can occur here is
	// racing cancellation against processing, which should be rare.
	lock sync.Mutex

	cancelError error
	cancelCtxs  []*AsyncCancelContext
}

func NewAsyncContext() *AsyncContext {
	return &AsyncContext{}
}

func ReleaseAsyncContext(ctx *AsyncContext) {
	*ctx = AsyncContext{
		cancelCtxs: ctx.cancelCtxs[:0],
	}
}

func (c *AsyncContext) Cancel() {
	c.internalCancel(context.Canceled)
}

func (c *AsyncContext) CancelForDeadline() {
	c.internalCancel(context.DeadlineExceeded)
}

func (c *AsyncContext) internalCancel(err error) {
	c.lock.Lock()

	// Check if we were already cancelled, and bail out if so.
	if c.cancelError != nil {
		c.lock.Unlock()
		return
	}

	c.cancelError = err

	// once cancelError is set, nobody is permitted to mutate the cancelCtxs
	// that exist on this AsyncContext, so its safe to steal the slice for use
	// outside of the lock.  The only caveat being that it isn't safe to put
	// this context back into the pool until we're done with that slice.
	cancelCtxs := c.cancelCtxs

	// we truncate the slice rather than setting it to nil so that we can keep
	// the underlying array allocation between uses of this AsyncContext.
	c.cancelCtxs = c.cancelCtxs[:0]

	c.lock.Unlock()

	for _, cancelCtx := range cancelCtxs {
		cancelCtx.internalCancel(err)
	}
}

func (c *AsyncContext) attachCancelContext(cc *AsyncCancelContext) {
	c.lock.Lock()
	c.cancelCtxs = append(c.cancelCtxs, cc)
	c.lock.Unlock()
}

func (c *AsyncContext) detachCancelContext(cc *AsyncCancelContext) bool {
	c.lock.Lock()

	foundIdx := -1
	for cancelCtxIdx, cancelCtx := range c.cancelCtxs {
		if cancelCtx == cc {
			foundIdx = cancelCtxIdx
			break
		}
	}

	// if it's not found, that means we already are in the process of calling
	// the cancellation context to cancel it...
	if foundIdx == -1 {
		c.lock.Unlock()
		return false
	}

	// because the ordering is irrelevant here, we can cheaply remove an item
	// from the cancel contexts list by displacing the last entry into the
	// slot we are removing, and shorten the slice by one.
	c.cancelCtxs[foundIdx] = c.cancelCtxs[len(c.cancelCtxs)-1]
	c.cancelCtxs = c.cancelCtxs[:len(c.cancelCtxs)-1]

	c.lock.Unlock()
	return true
}

/*
Creates a new cancellation context that is associated with this AsyncContext.  It
is the callers responsibility to ensure that OnCancel is invoked exactly once, and
that MarkComplete is invoked exactly once, unless the OnCancel handler is invoked
and returns true which indicates the operation was cancelled successfully and will
never complete.
*/
func (c *AsyncContext) WithCancellation() *AsyncCancelContext {
	cc := newAsyncCancelContext()
	cc.ctx = c
	c.attachCancelContext(cc)
	return cc
}

type AsyncCancelContext struct {
	lock       sync.Mutex
	ctx        *AsyncContext
	isComplete bool
	canceledFn func(err error) bool
}

func newAsyncCancelContext() *AsyncCancelContext {
	return &AsyncCancelContext{}
}

func releaseAsyncCancelContext(c *AsyncCancelContext) {
	*c = AsyncCancelContext{}
}

// internalCancel is invoked by cancellation of the parent AsyncContext, it is
// assumed that the parent context has already detatched this request.
func (c *AsyncCancelContext) internalCancel(err error) {
	c.lock.Lock()

	// we assume the parent context already detatched us, so first step is to
	// reflect that in our local state.
	c.ctx = nil

	// if we are already complete, nothing to do
	if c.isComplete {
		c.lock.Unlock()
		return
	}

	// TODO(brett19): There is actually a major race here, since the AsyncContext
	// releases its lock before it calls internalCancel, this means that it's possible
	// that the 'cleanup' of this AsyncCancelContext occurs after we grab this item from
	// the cancelCtx's list, but before this method is called.

	c.lock.Unlock()
}

func (c *AsyncCancelContext) MarkComplete() bool {
	c.lock.Lock()

	// if this is already marked complete, thats an error
	if c.isComplete {
		panic("multiple calls to AsyncCancelContext isComplete")
	}

	// if the canceledFn is nil, this indicates that OnCancel has not yet been invoked.
	// We simply mark the operation as having been completed so that OnCancel knows
	// it can release this cancel context back to the pool, once it will no longer
	// be used.
	if c.canceledFn == nil {
		// mark this cancel context as complete.
		c.isComplete = true

		// disassociate this cancel context from the parent context
		if c.ctx.detachCancelContext(c) {
			// we only clear our ctx reference when the parent context has successfully
			// detatched us.  if it returns false here, that means its in the process
			// of invoking our internalCancel function, which is information which is
			// needed into order to know when we can release this cancel context.
			c.ctx = nil
		}

		c.lock.Unlock()
		return true
	}

	// if the canceledFn is non-nil, this indicates that OnCancel has already been
	// invoked, which means no more calls to this AsyncCancelContext are valid and
	// it is safe to release it back to the pool.
	c.canceledFn = nil

	c.lock.Unlock()

	releaseAsyncCancelContext(c)

	return true
}

func (c *AsyncCancelContext) OnCancel(fn func(error) bool) {
	c.lock.Lock()

	// if there is a cancelfn registered already, thats an error
	if c.canceledFn != nil {
		panic("multiple calls to AsyncCancelContext OnCancel")
	}

	// if MarkComplete was already invoked, all we need to do is release
	// this cancel context back to the pool and we are done.
	if c.isComplete {
		c.lock.Unlock()
		releaseAsyncCancelContext(c)
		return
	}

	c.canceledFn = fn

	c.lock.Unlock()
}
