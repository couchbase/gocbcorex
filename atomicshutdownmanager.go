package gocbcorex

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
)

const MaxShutdownManagerOps = uint64(math.MaxUint32) + 1

type atomicShutdownManager struct {
	pendingOps  uint64
	lock        sync.Mutex
	hasShutdown bool
	shutdownCh  chan struct{}
}

func (m *atomicShutdownManager) NumPendingOps() uint64 {
	numPendingOps := atomic.LoadUint64(&m.pendingOps)
	return numPendingOps & (MaxShutdownManagerOps - 1)
}

func (m *atomicShutdownManager) IncrementOpCount() error {
	newPendingOps := atomic.AddUint64(&m.pendingOps, 1)
	if newPendingOps >= MaxShutdownManagerOps {
		m.DecrementOpCount()
		return ErrShutdown
	}
	return nil
}

func (m *atomicShutdownManager) DecrementOpCount() {
	newPendingOps := atomic.AddUint64(&m.pendingOps, ^uint64(0))
	if newPendingOps < MaxShutdownManagerOps {
		// we are not shutting down, we can leave early
		return
	}

	if newPendingOps > MaxShutdownManagerOps {
		// we are shut down, but there are still pending operations
		return
	}

	m.lock.Lock()
	if !m.hasShutdown {
		m.hasShutdown = true
		close(m.shutdownCh)
	}
	m.lock.Unlock()
}

func (m *atomicShutdownManager) Shutdown(ctx context.Context) error {
	m.lock.Lock()
	if m.shutdownCh == nil {
		m.shutdownCh = make(chan struct{})

		newPendingOps := atomic.AddUint64(&m.pendingOps, MaxShutdownManagerOps)
		if newPendingOps == MaxShutdownManagerOps {
			m.hasShutdown = true
			close(m.shutdownCh)
		}
	}
	shutdownCh := m.shutdownCh
	m.lock.Unlock()

	select {
	case <-shutdownCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
