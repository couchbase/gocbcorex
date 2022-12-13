package core

import (
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/gocbcore/v10"
)

type asyncCancelFn struct {
	wasInvoked uint32
	fn         func(error)
}

type asyncContext struct {
	cancelError unsafe.Pointer
	cancelFn    unsafe.Pointer
}

func NewAsyncContext() *asyncContext {
	return &asyncContext{}
}

func (c *asyncContext) Cancel() {
	c.InternalCancel(gocbcore.ErrRequestCanceled)
}

func (c *asyncContext) InternalCancel(err error) {
	if !atomic.CompareAndSwapPointer(&c.cancelError, nil, unsafe.Pointer(&err)) {
		return
	}

	cancelFn := (*asyncCancelFn)(atomic.LoadPointer(&c.cancelFn))
	if cancelFn != nil {
		if atomic.CompareAndSwapUint32(&cancelFn.wasInvoked, 0, 1) {
			cancelFn.fn(err)
		}
	}
}

func (c *asyncContext) OnCancel(fn func(error)) {
	cancelFn := &asyncCancelFn{
		fn:         fn,
		wasInvoked: 0,
	}
	if !atomic.CompareAndSwapPointer(&c.cancelFn, nil, unsafe.Pointer(cancelFn)) {
		panic("a bad thing")
	}

	err := (*error)(atomic.LoadPointer(&c.cancelError))
	if err != nil {
		if atomic.CompareAndSwapUint32(&cancelFn.wasInvoked, 0, 1) {
			cancelFn.fn(*err)
		}
	}
}

func (c *asyncContext) DropOnCancel() {
	// Should check here to ensure there is a cancelfn, otherwise this is invalid state that we should know about.
	atomic.StorePointer(&c.cancelFn, unsafe.Pointer(nil))
}
