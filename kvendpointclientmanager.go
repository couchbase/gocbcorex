package gocbcorex

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type KvEndpointClientManager interface {
	KvClientProvider
	KvEndpointClientProvider

	UpdateEndpoints(endpoints map[string]KvTarget, addOnly bool) error
	UpdateAuth(newAuth KvClientAuth)
	UpdateSelectedBucket(newBucket string)
	Close() error
}

type NewKvClientPoolFunc func(opts *KvClientPoolOptions) (KvClientPool, error)

type KvEndpointClientManagerOptions struct {
	Logger          *zap.Logger
	NewKvClientPool NewKvClientPoolFunc
	OnCloseHandler  func(KvEndpointClientManager)

	NumPoolConnections       uint
	OnDemandConnect          bool
	ConnectTimeout           time.Duration
	ConnectErrThrottlePeriod time.Duration
	BootstrapOpts            KvClientBootstrapOptions
	DcpOpts                  *KvClientDcpOptions
	DcpHandlers              KvClientDcpEventsHandlers

	Endpoints      map[string]KvTarget
	Auth           KvClientAuth
	SelectedBucket string
}

type kvEndpointClientManagerState struct {
	ClientPools map[string]KvClientPool
}

type kvEndpointClientManagerConfig struct {
	Endpoints      map[string]KvTarget
	Auth           KvClientAuth
	SelectedBucket string
}

type kvEndpointClientManager struct {
	logger                   *zap.Logger
	newKvClientPool          NewKvClientPoolFunc
	onCloseHandler           func(KvEndpointClientManager)
	numPoolConnections       uint
	onDemandConnect          bool
	connectTimeout           time.Duration
	connectErrThrottlePeriod time.Duration
	bootstrapOpts            KvClientBootstrapOptions
	dcpOpts                  *KvClientDcpOptions
	dcpHandlers              KvClientDcpEventsHandlers

	lock           sync.Mutex
	auth           KvClientAuth
	selectedBucket string
	clientPools    map[string]KvClientPool

	state atomic.Pointer[kvEndpointClientManagerState]
}

var _ (KvEndpointClientManager) = (*kvEndpointClientManager)(nil)

func NewKvEndpointClientManager(
	opts *KvEndpointClientManagerOptions,
) (KvEndpointClientManager, error) {
	if opts == nil {
		opts = &KvEndpointClientManagerOptions{}
	}

	if opts.NumPoolConnections == 0 {
		opts.NumPoolConnections = 1
	}

	logger := loggerOrNop(opts.Logger)
	// We namespace the pool to improve debugging,
	logger = logger.With(
		zap.String("mgrId", uuid.NewString()[:8]),
	)

	newKvClientPool := opts.NewKvClientPool
	if newKvClientPool == nil {
		newKvClientPool = NewKvClientPool
	}

	mgr := &kvEndpointClientManager{
		logger:                   logger,
		newKvClientPool:          newKvClientPool,
		onCloseHandler:           opts.OnCloseHandler,
		numPoolConnections:       opts.NumPoolConnections,
		onDemandConnect:          opts.OnDemandConnect,
		connectTimeout:           opts.ConnectTimeout,
		connectErrThrottlePeriod: opts.ConnectErrThrottlePeriod,
		bootstrapOpts:            opts.BootstrapOpts,
		dcpOpts:                  opts.DcpOpts,
		dcpHandlers:              opts.DcpHandlers,

		clientPools: make(map[string]KvClientPool),
	}

	mgr.auth = opts.Auth
	mgr.selectedBucket = opts.SelectedBucket
	err := mgr.UpdateEndpoints(opts.Endpoints, false)
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

func (m *kvEndpointClientManager) UpdateEndpoints(endpoints map[string]KvTarget, addOnly bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	oldPools := make(map[string]KvClientPool)
	for poolName, pool := range m.clientPools {
		oldPools[poolName] = pool
	}

	newPools := make(map[string]KvClientPool)

	for endpoint, target := range endpoints {
		var pool KvClientPool

		oldPool := oldPools[endpoint]
		if oldPool != nil {
			oldPool.UpdateTarget(target)

			pool = oldPool
			delete(oldPools, endpoint)
		} else {
			newPool, err := m.newKvClientPool(&KvClientPoolOptions{
				Logger:                   m.logger.Named("pool"),
				NumConnections:           m.numPoolConnections,
				OnDemandConnect:          m.onDemandConnect,
				ConnectTimeout:           m.connectTimeout,
				ConnectErrThrottlePeriod: m.connectErrThrottlePeriod,
				BootstrapOpts:            m.bootstrapOpts,
				DcpOpts:                  m.dcpOpts,
				DcpHandlers:              m.dcpHandlers,

				Target:         target,
				Auth:           m.auth,
				SelectedBucket: m.selectedBucket,
			})
			if err != nil {
				m.logger.Debug("failed to create pool", zap.Error(err))
				continue
			}

			pool = newPool
		}

		newPools[endpoint] = pool
	}

	if addOnly {
		// in add-only mode, we keep any existing pools that aren't in the new set
		// this is useful for making sure all routers still work until we've updated
		// the routers separately...
		for endpoint, pool := range oldPools {
			newPools[endpoint] = pool
			delete(oldPools, endpoint)
		}
	}

	for _, pool := range oldPools {
		if err := pool.Close(); err != nil {
			m.logger.Debug("failed to close pool", zap.Error(err))
		}
	}

	m.clientPools = newPools
	m.state.Store(&kvEndpointClientManagerState{
		ClientPools: newPools,
	})

	return nil
}

func (m *kvEndpointClientManager) UpdateAuth(newAuth KvClientAuth) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, pool := range m.clientPools {
		pool.UpdateAuth(newAuth)
	}
}

func (m *kvEndpointClientManager) UpdateSelectedBucket(newBucket string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, pool := range m.clientPools {
		pool.UpdateSelectedBucket(newBucket)
	}
}

func (m *kvEndpointClientManager) getState() (*kvEndpointClientManagerState, error) {
	state := m.state.Load()
	if state == nil {
		return nil, errors.New("kv endpoint client manager is closed")
	}

	return state, nil
}

func (m *kvEndpointClientManager) GetClient(ctx context.Context) (KvClient, error) {
	state, err := m.getState()
	if err != nil {
		return nil, err
	}

	// Just pick one at random for now
	for _, pool := range state.ClientPools {
		return pool.GetClient(ctx)
	}

	return nil, ErrInvalidEndpoint
}

func (m *kvEndpointClientManager) GetEndpointClient(ctx context.Context, endpoint string) (KvClient, error) {
	if endpoint == "" {
		return nil, placeholderError{"endpoint must be specified for GetEndpoint"}
	}

	state, err := m.getState()
	if err != nil {
		return nil, err
	}

	pool, ok := state.ClientPools[endpoint]
	if !ok {
		var validKeys []string
		for validEndpoint := range state.ClientPools {
			validKeys = append(validKeys, validEndpoint)
		}
		return nil, placeholderError{fmt.Sprintf("endpoint not known `%s` in %+v", endpoint, validKeys)}
	}

	return pool.GetClient(ctx)
}

func (m *kvEndpointClientManager) Close() error {
	m.logger.Info("closing kv endpoint client manager")

	m.state.Store(nil)

	m.lock.Lock()
	defer m.lock.Unlock()

	m.state.Store(nil)

	for _, pool := range m.clientPools {
		if err := pool.Close(); err != nil {
			m.logger.Debug("Failed to close kv client pool", zap.Error(err))
		}
	}
	m.clientPools = make(map[string]KvClientPool)

	if m.onCloseHandler != nil {
		m.onCloseHandler(m)
	}

	m.logger.Info("closed kv endpoint client manager")

	return nil
}
