package gocbcorex

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/exp/slices"

	"go.uber.org/zap"
)

var (
	ErrPoolStillConnecting = contextualDeadline{"still waiting for a pool connection to be established"}
)

type NewKvClientManagerFunc func(*KvClientManagerOptions) KvClientManager

type KvClientPool interface {
	KvClientManager
}

type KvClientPoolOptions struct {
	Logger             *zap.Logger
	NewKvClientManager NewKvClientManagerFunc

	NumConnections           uint
	OnDemandConnect          bool
	ConnectTimeout           time.Duration
	ConnectErrThrottlePeriod time.Duration
	BootstrapOpts            KvClientBootstrapOptions
	DcpOpts                  *KvClientDcpOptions
	DcpHandlers              KvClientDcpEventsHandlers

	Target         KvTarget
	Auth           KvClientAuth
	SelectedBucket string
}

type kvClientPoolFastMap struct {
	activeClients []KvClient
}

type kvClientPoolEntry struct {
	Manager KvClientManager
	Client  KvClient
	Err     error
}

type kvClientPool struct {
	logger *zap.Logger

	clientIdx uint64
	fastMap   atomic.Pointer[kvClientPoolFastMap]

	// this lock only controls the values inside the manages entries
	lock     sync.Mutex
	isClosed bool
	poolName string
	managers []*kvClientPoolEntry
}

func NewKvClientPool(opts *KvClientPoolOptions) (KvClientPool, error) {
	if opts == nil {
		opts = &KvClientPoolOptions{}
	}

	if opts.NumConnections == 0 {
		return nil, errors.New("a pool of connections must have at least one connection")
	}

	logger := loggerOrNop(opts.Logger)
	// We namespace the pool to improve debugging,
	logger = logger.With(
		zap.String("poolId", uuid.NewString()[:8]),
	)

	connectTimeout := opts.ConnectTimeout
	if connectTimeout == 0 {
		connectTimeout = 10 * time.Second
	}

	connectErrThrottlePeriod := opts.ConnectErrThrottlePeriod
	if connectErrThrottlePeriod == 0 {
		connectErrThrottlePeriod = 1 * time.Second
	}

	newKvClientManager := opts.NewKvClientManager
	if newKvClientManager == nil {
		newKvClientManager = NewKvClientManager
	}

	// TODO(brett19): rewrite this
	poolName := opts.Target.Address
	if opts.SelectedBucket != "" {
		poolName += "/" + opts.SelectedBucket
	}

	p := &kvClientPool{
		logger: logger,

		poolName: poolName,
	}

	for clientIdx := uint(0); clientIdx < opts.NumConnections; clientIdx++ {
		manager := newKvClientManager(&KvClientManagerOptions{
			Logger:                   p.logger,
			OnDemandConnect:          opts.OnDemandConnect,
			ConnectTimeout:           opts.ConnectTimeout,
			ConnectErrThrottlePeriod: opts.ConnectErrThrottlePeriod,
			StateChangeHandler:       p.handleProviderStateChange,
			DcpHandlers:              opts.DcpHandlers,
			BootstrapOpts:            opts.BootstrapOpts,
			DcpOpts:                  opts.DcpOpts,
			Target:                   opts.Target,
			Auth:                     opts.Auth,
			SelectedBucket:           opts.SelectedBucket,
		})

		p.managers = append(p.managers, &kvClientPoolEntry{
			Manager: manager,
		})
	}

	p.rebuildFastMapLocked()

	return p, nil
}

func (p *kvClientPool) handleProviderStateChange(manager KvClientManager, client KvClient, err error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	entryIdx := slices.IndexFunc(p.managers, func(entry *kvClientPoolEntry) bool {
		return entry.Manager == manager
	})
	if entryIdx == -1 {
		return
	}

	entry := p.managers[entryIdx]
	entry.Client = client
	entry.Err = err

	p.rebuildFastMapLocked()
}

func (p *kvClientPool) rebuildFastMapLocked() {
	clients := make([]KvClient, 0, len(p.managers))

	if !p.isClosed {
		for _, entry := range p.managers {
			if entry.Client != nil {
				clients = append(clients, entry.Client)
			}
		}
	}

	p.fastMap.Store(&kvClientPoolFastMap{
		activeClients: clients,
	})
}

func (p *kvClientPool) UpdateTarget(newTarget KvTarget) {
	for _, entry := range p.managers {
		entry.Manager.UpdateTarget(newTarget)
	}
}

func (p *kvClientPool) UpdateAuth(newAuth KvClientAuth) {
	for _, entry := range p.managers {
		entry.Manager.UpdateAuth(newAuth)
	}
}

func (p *kvClientPool) UpdateSelectedBucket(newBucket string) {
	for _, entry := range p.managers {
		entry.Manager.UpdateSelectedBucket(newBucket)
	}
}

func (p *kvClientPool) GetClient(ctx context.Context) (KvClient, error) {
	fastMap := p.fastMap.Load()
	if fastMap != nil {
		fastMapNumConns := uint64(len(fastMap.activeClients))
		if fastMapNumConns > 0 {
			clientIdx := atomic.AddUint64(&p.clientIdx, 1) - 1
			conn := fastMap.activeClients[clientIdx%fastMapNumConns]
			return conn, nil
		}
	}

	p.logger.Debug("no client found in fast map")

	return p.getClientSlow(ctx)
}

func (p *kvClientPool) getClientSlow(ctx context.Context) (KvClient, error) {
	p.lock.Lock()
	isClosed := p.isClosed
	p.lock.Unlock()

	if isClosed {
		return nil, net.ErrClosed
	}

	// TODO(brett19): Improve this later to wait for _any_ connection to come up.
	clientIdx := atomic.AddUint64(&p.clientIdx, 1) - 1
	numManagers := uint64(len(p.managers))
	client, err := p.managers[clientIdx%numManagers].Manager.GetClient(ctx)
	if err != nil {
		if errors.Is(err, ErrClientStillConnecting) {
			return nil, ErrPoolStillConnecting
		}
		return nil, err
	}

	return client, nil
}

func (p *kvClientPool) Close() error {
	p.logger.Debug("closing kv client pool")

	p.lock.Lock()
	p.isClosed = true
	p.rebuildFastMapLocked()
	p.lock.Unlock()

	for _, entry := range p.managers {
		err := entry.Manager.Close()
		if err != nil {
			p.logger.Warn("error closing kv client manager", zap.Error(err))
		}
	}

	return nil
}
