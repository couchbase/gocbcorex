package core

import (
	"sync/atomic"
	"unsafe"
)

// An AtomicPointer is an atomic pointer of type *T. The zero value is a nil *T.
// Temporary type until Go 1.19 is adopted.
type AtomicPointer[T any] struct {
	// Mention T in a field to disallow conversion between Pointer types.
	// See go.dev/issue/56603 for more details.
	_ [0]T

	v unsafe.Pointer
}

// Load atomically loads and returns the value stored in x.
func (x *AtomicPointer[T]) Load() *T { return (*T)(atomic.LoadPointer(&x.v)) }

// Store atomically stores val into x.
func (x *AtomicPointer[T]) Store(val *T) { atomic.StorePointer(&x.v, unsafe.Pointer(val)) }

// Swap atomically stores new into x and returns the previous value.
func (x *AtomicPointer[T]) Swap(new *T) (old *T) {
	return (*T)(atomic.SwapPointer(&x.v, unsafe.Pointer(new)))
}

// CompareAndSwap executes the compare-and-swap operation for x.
func (x *AtomicPointer[T]) CompareAndSwap(old, new *T) (swapped bool) {
	return atomic.CompareAndSwapPointer(&x.v, unsafe.Pointer(old), unsafe.Pointer(new))
}
