package gocbcorex

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAtomciShutdownManagerNoOps(t *testing.T) {
	m := atomicShutdownManager{}
	err := m.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestAtomciShutdownManagerOldOps(t *testing.T) {
	m := atomicShutdownManager{}
	require.NoError(t, m.IncrementOpCount())
	require.NoError(t, m.IncrementOpCount())
	m.DecrementOpCount()
	m.DecrementOpCount()
	err := m.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestAtomciShutdownManagerDelayedOps(t *testing.T) {
	m := atomicShutdownManager{}
	require.NoError(t, m.IncrementOpCount())
	go func() {
		time.Sleep(10 * time.Millisecond)
		m.DecrementOpCount()
	}()
	err := m.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestAtomciShutdownManagerLateOps(t *testing.T) {
	m := atomicShutdownManager{}
	err := m.Shutdown(context.Background())
	require.NoError(t, err)

	require.ErrorIs(t, m.IncrementOpCount(), ErrShutdown)

	err = m.Shutdown(context.Background())
	require.NoError(t, err)
}
