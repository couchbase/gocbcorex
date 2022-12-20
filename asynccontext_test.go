package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAsyncContextCancelNoOps(t *testing.T) {
	ctx := NewAsyncContext()
	ctx.Cancel()
}

func TestAsyncContextCancelOneOp(t *testing.T) {
	numCancelCalls := 0

	ctx := NewAsyncContext()

	cancelCtx := ctx.WithCancellation()
	cancelCtx.OnCancel(func(err error) bool {
		numCancelCalls++
		return true
	})

	ctx.Cancel()

	require.Equal(t, 1, numCancelCalls)
}

func TestAsyncContextCancelTwoOp(t *testing.T) {
	numCancelCalls1 := 0
	numCancelCalls2 := 0

	ctx := NewAsyncContext()

	cancelCtx1 := ctx.WithCancellation()
	cancelCtx1.OnCancel(func(err error) bool {
		numCancelCalls1++
		return true
	})

	cancelCtx2 := ctx.WithCancellation()
	cancelCtx2.OnCancel(func(err error) bool {
		numCancelCalls2++
		return true
	})

	ctx.Cancel()

	require.Equal(t, 1, numCancelCalls1)
	require.Equal(t, 1, numCancelCalls2)
}

func TestAsyncContextCancelAfterCompleteBeforeOnCancel(t *testing.T) {
	numCancelCalls := 0

	ctx := NewAsyncContext()

	cancelCtx := ctx.WithCancellation()
	shouldRun := cancelCtx.MarkComplete()
	ctx.Cancel()
	cancelCtx.OnCancel(func(err error) bool {
		numCancelCalls++
		return true
	})

	require.Equal(t, true, shouldRun)
	require.Equal(t, 0, numCancelCalls)
}

func TestAsyncContextCancelAfterComplete(t *testing.T) {
	numCancelCalls1 := 0
	shouldRun1 := false
	numCancelCalls2 := 0
	shouldRun2 := false

	ctx := NewAsyncContext()

	cancelCtx1 := ctx.WithCancellation()
	cancelCtx1.OnCancel(func(err error) bool {
		numCancelCalls1++
		return true
	})
	shouldRun1 = cancelCtx1.MarkComplete()

	cancelCtx2 := ctx.WithCancellation()
	cancelCtx2.OnCancel(func(err error) bool {
		numCancelCalls2++
		return true
	})
	shouldRun2 = cancelCtx2.MarkComplete()

	ctx.Cancel()

	require.Equal(t, true, shouldRun1)
	require.Equal(t, 0, numCancelCalls1)
	require.Equal(t, true, shouldRun2)
	require.Equal(t, 0, numCancelCalls2)
}

func TestAsyncContextCreateAfterCancel(t *testing.T) {
	numCancelCalls := 0

	ctx := NewAsyncContext()

	ctx.Cancel()

	cancelCtx := ctx.WithCancellation()
	cancelCtx.OnCancel(func(err error) bool {
		numCancelCalls++
		return true
	})

	require.Equal(t, 1, numCancelCalls)
}

func TestAsyncContextCreateCompleteErrorAfterCancel(t *testing.T) {
	ctx := NewAsyncContext()

	ctx.Cancel()

	numCancelCalls := 0
	cancelCtx := ctx.WithCancellation()
	shouldRun := cancelCtx.MarkComplete()
	cancelCtx.OnCancel(func(err error) bool {
		numCancelCalls++
		return false
	})

	require.Equal(t, false, shouldRun)
	require.Equal(t, 1, numCancelCalls)
}

func TestAsyncContextCreateErrorCompleteAfterCancel(t *testing.T) {
	ctx := NewAsyncContext()

	ctx.Cancel()

	numCancelCalls := 0
	cancelCtx := ctx.WithCancellation()
	cancelCtx.OnCancel(func(err error) bool {
		numCancelCalls++
		return false
	})
	shouldRun := cancelCtx.MarkComplete()

	require.Equal(t, false, shouldRun)
	require.Equal(t, 1, numCancelCalls)
}

func TestAsyncContextInternalCancel(t *testing.T) {
	numCancelCalls1 := 0
	shouldRun1 := false
	numCancelCalls2 := 0
	shouldRun2 := false

	ctx := NewAsyncContext()

	cancelCtx1 := ctx.WithCancellation()
	cancelCtx2 := ctx.WithCancellation()

	ctx.markCanceled(context.Canceled)

	cancelCtx1.OnCancel(func(err error) bool {
		numCancelCalls1++
		return false
	})
	cancelCtx1.internalCancel(context.Canceled)
	shouldRun1 = cancelCtx1.MarkComplete()

	cancelCtx2.OnCancel(func(err error) bool {
		numCancelCalls2++
		return false
	})
	shouldRun2 = cancelCtx2.MarkComplete()
	cancelCtx2.internalCancel(context.Canceled)

	require.Equal(t, false, shouldRun1)
	require.Equal(t, 1, numCancelCalls1)
	require.Equal(t, false, shouldRun2)
	require.Equal(t, 1, numCancelCalls2)
}

func TestAsyncContextInternalCancelAfterComplete(t *testing.T) {
	numCancelCalls1 := 0
	shouldRun1 := false
	numCancelCalls2 := 0
	shouldRun2 := false

	ctx := NewAsyncContext()

	cancelCtx1 := ctx.WithCancellation()
	cancelCtx2 := ctx.WithCancellation()

	ctx.markCanceled(context.Canceled)

	shouldRun1 = cancelCtx1.MarkComplete()
	cancelCtx1.internalCancel(context.Canceled)
	cancelCtx1.OnCancel(func(err error) bool {
		numCancelCalls1++
		return false
	})

	shouldRun2 = cancelCtx2.MarkComplete()
	cancelCtx2.OnCancel(func(err error) bool {
		numCancelCalls2++
		return false
	})
	cancelCtx2.internalCancel(context.Canceled)

	require.Equal(t, true, shouldRun1)
	require.Equal(t, 0, numCancelCalls1)
	require.Equal(t, true, shouldRun2)
	require.Equal(t, 1, numCancelCalls2)
}
