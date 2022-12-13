package core

import (
	"testing"

	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/require"
)

func TestAsyncContextNilCancel(t *testing.T) {
	ctx := &asyncContext{}

	ctx.Cancel()
}

func TestAsyncContextCancelBeforeOnCancel(t *testing.T) {
	ctx := &asyncContext{}

	ctx.Cancel()

	var called int
	var err error
	fn := func(e error) {
		called++
		err = e
	}

	ctx.OnCancel(fn)

	require.Equal(t, 1, called)
	require.ErrorIs(t, err, gocbcore.ErrRequestCanceled)
}

func TestAsyncContextCancelAfterOnCancel(t *testing.T) {
	ctx := &asyncContext{}

	var called int
	var err error
	fn := func(e error) {
		called++
		err = e
	}

	ctx.OnCancel(fn)
	require.Equal(t, 0, called)

	ctx.Cancel()
	require.Equal(t, 1, called)

	ctx.Cancel()
	require.Equal(t, 1, called)

	require.ErrorIs(t, err, gocbcore.ErrRequestCanceled)
}

func TestAsyncContextInternalCancel(t *testing.T) {
	ctx := &asyncContext{}

	var called int
	var err error
	fn := func(e error) {
		called++
		err = e
	}

	ctx.OnCancel(fn)
	require.Equal(t, 0, called)
	require.Nil(t, err)

	ctx.InternalCancel(gocbcore.ErrUnambiguousTimeout)

	require.Equal(t, 1, called)
	require.ErrorIs(t, err, gocbcore.ErrUnambiguousTimeout)
}
