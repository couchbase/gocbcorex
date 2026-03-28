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

type NewKvClientBabysitterFunc func(*KvClientBabysitterOptions) KvClientBabysitter

type KvClientPool interface {
	KvClientBabysitter
}

type KvClientPoolOptions struct {
	Logger                *zap.Logger
	NewKvClientBabysitter NewKvClientBabysitterFunc

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
	Manager KvClientBabysitter
	Client  KvClient
	Err     error
}

type kvClientPool struct {
	logger *zap.Logger

	clientIdx uint64
	fastMap   atomic.Pointer[kvClientPoolFastMap]

	// this lock only controls the values inside the manages entries
	lock          sync.Mutex
	stateUpdateCh chan struct{}
	isClosed      bool
	managers      []*kvClientPoolEntry
}

func NewKvClientPool(opts *KvClientPoolOptions) (KvClientPool, error) {
	if opts == nil {
		opts = &KvClientPoolOptions{}
	}

	if opts.NumConnections == 0 {
		return nil, errors.New("a pool of connections must have at least one connection")
	}

	if opts.OnDemandConnect {
		if opts.NumConnections != 1 {
			return nil, errors.New("on-demand connect pools can only have one connection")
		}
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

	newKvClientBabysitter := opts.NewKvClientBabysitter
	if newKvClientBabysitter == nil {
		newKvClientBabysitter = NewKvClientBabysitter
	}

	p := &kvClientPool{
		logger:        logger,
		stateUpdateCh: make(chan struct{}, 1),
	}

	for clientIdx := uint(0); clientIdx < opts.NumConnections; clientIdx++ {
		manager := newKvClientBabysitter(&KvClientBabysitterOptions{
			Logger:                   p.logger,
			OnDemandConnect:          opts.OnDemandConnect,
			ConnectTimeout:           connectTimeout,
			ConnectErrThrottlePeriod: connectErrThrottlePeriod,
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

	// We need to use the lock here because the babysitters above are
	// potentially going to call their state change handler asynchronously
	// before we return from this function.
	p.lock.Lock()
	defer p.lock.Unlock()

	p.rebuildFastMapLocked()

	return p, nil
}

func (p *kvClientPool) handleProviderStateChange(manager KvClientBabysitter, client KvClient, err error) {
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

	// signal the state update channel
	close(p.stateUpdateCh)
	p.stateUpdateCh = make(chan struct{}, 1)

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

	return p.getClientSlow(ctx)
}

func (p *kvClientPool) getClientSlow(ctx context.Context) (KvClient, error) {
	// if there is only 1 manager, we can skip the complex distributing
	// logic and just directly call its GetClient method.  This is especially
	// important in the OnDemandConnect case where we only have 1 manager
	// and we need to actually trigger the build of the connection.
	if len(p.managers) == 1 {
		return p.managers[0].Manager.GetClient(ctx)
	}

	clientIdxStart := atomic.AddUint64(&p.clientIdx, 1) - 1

	p.lock.Lock()

	for {
		if p.isClosed {
			p.lock.Unlock()
			return nil, net.ErrClosed
		}

		numManagers := len(p.managers)

		// using the clientIdxStart as a starting point, loop all the managers
		// to try and find one which has a client we can use.
		for clientNum := 0; clientNum < numManagers; clientNum++ {
			clientIdx := (clientIdxStart + uint64(clientNum)) % uint64(numManagers)
			entry := p.managers[clientIdx]
			if entry.Client != nil {
				p.lock.Unlock()
				return entry.Client, nil
			}
		}

		// if we get here, then no clients are available yet.  next check if any
		// clients have explicitly errored, and return that if they have
		for clientNum := 0; clientNum < numManagers; clientNum++ {
			entry := p.managers[clientNum]
			if entry.Err != nil {
				p.lock.Unlock()
				return nil, entry.Err
			}
		}

		// if we still have no clients, then wait for a state change
		stateUpdateCh := p.stateUpdateCh
		p.lock.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-stateUpdateCh:
			// continue the loop to re-check for clients
		}

		p.lock.Lock()
	}
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
