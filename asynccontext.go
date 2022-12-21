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
	c.cancelWithError(context.Canceled)
}

func (c *AsyncContext) CancelForDeadline() {
	c.cancelWithError(context.DeadlineExceeded)
}

func (c *AsyncContext) cancelWithError(err error) {
	// reminder: this function is separated apart from the actual invocation of
	// the cancel context cancellations so that our testing can validate the race
	// interactions between the two structures.

	cancelCtxs := c.markCanceled(err)

	for _, cancelCtx := range cancelCtxs {
		cancelCtx.internalCancel(err)
	}
}

func (c *AsyncContext) markCanceled(err error) []*AsyncCancelContext {
	c.lock.Lock()

	// Check if we were already cancelled, and bail out if so.
	if c.cancelError != nil {
		c.lock.Unlock()
		return nil
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

	return cancelCtxs
}

/*
attachCancelContext attaches a new cancel context to this context.  If this
context is already cancelled, we immediately propagate that state to the
underlying cancel context instead since it's illegal for us to add new cancel
contexts to the async context after it's been cancelled.

Note that this function assumes that we have exclusive ownership of the cancel
context and that none of its methods could be called during this method call.
This is important because we don't use the cancel contexts's locks to mutate
it's state here.
*/
func (c *AsyncContext) attachCancelContext(cc *AsyncCancelContext) {
	c.lock.Lock()

	cancelError := c.cancelError

	// this if branch acts a completely distinct branch of the unlocking
	// because in this case, the ownership of the cc isn't ever transferred
	// into the context and we don't need to deal with concurrent callers to
	// the cancel context from the context (since it's never exposed to it)
	if cancelError != nil {
		c.lock.Unlock()

		cc.ctx = nil
		cc.cancelError = cancelError

		return
	}

	// reminder: putting this cc into the cancel contexts list means we no
	// longer have exclusive ownership of the cancel context (once we
	// have unlocked the mutex).
	cc.ctx = c
	c.cancelCtxs = append(c.cancelCtxs, cc)

	c.lock.Unlock()
}

/*
detatchCancelContext removes a cancel context from this async context.  we
return a boolean indicating whether the operation was successful to deal with
the race that can happen between a cancellation removing the cancel context
from the list, and the actual invokation of the cancel context's internalCancel.
*/
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
	c.attachCancelContext(cc)
	return cc
}

type AsyncCancelContext struct {
	lock        sync.Mutex
	ctx         *AsyncContext
	cancelError error
	isComplete  bool
	canceledFn  func(err error) bool
}

func newAsyncCancelContext() *AsyncCancelContext {
	return &AsyncCancelContext{}
}

func releaseAsyncCancelContext(cc *AsyncCancelContext) {
	*cc = AsyncCancelContext{}
}

// internalCancel is invoked by cancellation of the parent AsyncContext, it is
// assumed that the parent context has already detatched this request.
func (cc *AsyncCancelContext) internalCancel(err error) {
	cc.lock.Lock()

	// we assume the parent context already detatched us, so first step is to
	// reflect everything into our local state, since we can no longer rely on
	// the async context still being alive (it might be returned to a pool).
	cc.ctx = nil
	cc.cancelError = err

	// if canceledFn is nil, it means we haven't received an OnCancel call yet
	// and need to wait for that before we can do anything.
	if cc.canceledFn == nil {
		// reminder: ctx is irrelevant here because if MarkComplete had been successful
		// we would not actually be in this call to internalCancel (since MarkComplete
		// preempts it to be successful).
		// isComplete is also irrelevant here because MarkComplete can't possibly
		// have returned true without also pre-empting this call to internalCancel.

		cc.lock.Unlock()
		return
	}

	// we unlock the lock before calling the cancellation function in case it
	// blocks, since we don't want to block the parent context from it.
	wasCompleted := cc.isComplete
	cc.lock.Unlock()

	wasCanceled := cc.canceledFn(err)

	// if we successfully cancelled the operation, it means we can safely
	// release it, as the context no longer points to it, OnCancel was called
	// and MarkComplete will not be called.
	if wasCanceled {
		// this is just an extra debug check to try and reduce bugs...  its
		// obviously invalid if the MarkComplete call was invoked before we
		// even called the cancel function, but the cancel function says that
		// MarkComplete should never be called.
		if wasCompleted {
			panic("cancel function indicated successful cancellation after MarkCompleted called")
		}

		releaseAsyncCancelContext(cc)
		return
	}

	// if we see that the MarkComplete call was already made, we can also
	// safely release it, since all expected calls are completed.
	if wasCompleted {
		releaseAsyncCancelContext(cc)
		return
	}

	// if we get here, it means that we are expecting a MarkComplete call to
	// be invoked shortly, so we need to keep the cancel context around for now.
}

func (cc *AsyncCancelContext) MarkComplete() bool {
	cc.lock.Lock()

	// if this is already marked complete, thats an error
	if cc.isComplete {
		panic("multiple calls to AsyncCancelContext isComplete")
	}

	// mark this cancel context as complete.
	cc.isComplete = true

	// if our parent context is nil, it means that we must have already been
	// cancelled and we should return false to not do any more work.
	// reminder: this is especially important since we can't refer to our parent
	// context after we've been cancelled, since it might have been released.
	if cc.ctx == nil {
		// if canceledFn is not set, it means OnCancel still needs to be invoked,
		// so we need to wait for that before releasing ourselves.
		if cc.canceledFn == nil {
			cc.lock.Unlock()
			return false
		}

		// if its already set, OnCancel was already invoked and we are safe
		// to release this object back to the pool
		cc.lock.Unlock()
		releaseAsyncCancelContext(cc)
		return false
	}

	// disassociate this cancel context from the parent context
	if !cc.ctx.detachCancelContext(cc) {
		// if we fail to remove ourselves from our parent context, that means
		// that there is a pending internalCancel call coming, and we should
		// not do any more work, so just return false...
		cc.lock.Unlock()
		return false
	}

	// if detatchCancelContext succeeded, it means we no longer are owned by
	// the parent context anymore and can no longer refer to it.
	cc.ctx = nil

	// if we've successfully detatched ourselves from our parent context, but
	// canceledFn isn't set yet, we need to wait for the OnCancel call in order
	// to release ourselves...
	if cc.canceledFn == nil {
		cc.lock.Unlock()
		return true
	}

	cc.lock.Unlock()
	releaseAsyncCancelContext(cc)
	return true
}

func (cc *AsyncCancelContext) OnCancel(fn func(error) bool) {
	cc.lock.Lock()

	// if there is a canceledFn registered already, thats an error
	if cc.canceledFn != nil {
		panic("multiple calls to AsyncCancelContext OnCancel")
	}

	// we set canceledFn here to signal that OnCancel has been invoked.
	cc.canceledFn = fn

	// if isComplete is set, it means MarkComplete was already invoked.
	if cc.isComplete {
		// if the context is not nil, it means we failed disassociation and are still
		// waiting for the internalCancel call to come in.
		if cc.ctx != nil {
			cc.lock.Unlock()
			return
		}

		// if cancelError is set, it means that we received the cancellation before
		// MarkComplete was called (otherwise internalCancel doesn't set it).  Thus
		// we need to invoke the cancel handler here.
		if cc.cancelError != nil {
			cc.lock.Unlock()

			wasCanceled := cc.canceledFn(cc.cancelError)
			if wasCanceled {
				// this is a debug check, we already know MarkComplete was called, so having
				// the handler tell us it won't be called is obviously a logic error...
				panic("cancel function returned successful cancellation after MarkComplete invoked")
			}

			releaseAsyncCancelContext(cc)
			return
		}

		// if we get here, MarkComplete was already invoked and marked the operation as successful,
		// and the ctx is nil so we won't be called by the parent context anymore.  So we are safe
		// to release ourselves back to the pool.

		cc.lock.Unlock()
		releaseAsyncCancelContext(cc)
		return
	}

	// if our context reference is nil, it implies that someone has already cancelled
	// our operation and we have not seen a MarkComplete call yet.  We should be able
	// to immediately invoke the cancel handler.
	if cc.ctx == nil {
		// if the cancel error is not set, and ctx is nil, it implies that the operation
		// must have succeeded, but this is not the case since isComplete is false...
		if cc.cancelError == nil {
			panic("expected a cancel error for a nil context")
		}

		wasCanceled := fn(cc.cancelError)
		if !wasCanceled {
			// if we failed to cancel the operation, we expect to see a MarkComplete call
			// in the future, so we cannot release ourselves yet.
			cc.lock.Unlock()
			return
		}

		// if the function returns true, it means that the operation was succesfully
		// cancelled and isComplete is effectively marked as true at this point.
		// which also means (since we are in the ctx==nil branch) that no more calls
		// will be made to this cancel context and it can be released.
		cc.lock.Unlock()
		releaseAsyncCancelContext(cc)
		return
	}

	// if we get to this point, it means that OnCancel was the first thing that was
	// invoked and we could get cancelled or complete, nothing special to do here...

	cc.lock.Unlock()
}
