package gocbcorex

import (
	"context"
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

	Reconfigure(config KvEndpointClientManagerConfig)
	Close() error
}

type NewKvClientPoolFunc func(opts *KvClientPoolOptions) (KvClientPool, error)

type KvEndpointClientManagerConfigClient struct {
	KvClientPoolConfig
}

type KvEndpointClientManagerConfig struct {
	Clients map[string]KvEndpointClientManagerConfigClient
}

type KvEndpointClientManagerOptions struct {
	Logger          *zap.Logger
	NewKvClientPool NewKvClientPoolFunc
	OnCloseHandler  func(KvEndpointClientManager)

	NumPoolConnections       uint
	OnDemandConnect          bool
	ConnectTimeout           time.Duration
	ConnectErrThrottlePeriod time.Duration

	KvEndpointClientManagerConfig
}

type kvEndpointClientManagerPool struct {
	Config KvEndpointClientManagerConfigClient
	Pool   KvClientPool
}

type kvEndpointClientManagerState struct {
	ClientPools map[string]*kvEndpointClientManagerPool
}

type kvEndpointClientManager struct {
	logger             *zap.Logger
	newKvClientPool    NewKvClientPoolFunc
	onCloseHandler     func(KvEndpointClientManager)
	numPoolConnections uint

	lock          sync.Mutex
	currentConfig KvEndpointClientManagerConfig
	state         atomic.Pointer[kvEndpointClientManagerState]
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
		logger:             logger,
		newKvClientPool:    newKvClientPool,
		onCloseHandler:     opts.OnCloseHandler,
		numPoolConnections: opts.NumPoolConnections,
	}

	// initialize the client manager to having no clients
	mgr.currentConfig = KvEndpointClientManagerConfig{}
	mgr.Reconfigure(opts.KvEndpointClientManagerConfig)

	return mgr, nil
}

func (m *kvEndpointClientManager) Reconfigure(config KvEndpointClientManagerConfig) {
	m.lock.Lock()
	defer m.lock.Unlock()

	state := m.state.Load()
	if state == nil {
		state = &kvEndpointClientManagerState{}
	}

	m.logger.Debug("reconfiguring")

	oldPools := make(map[string]*kvEndpointClientManagerPool)
	for poolName, pool := range state.ClientPools {
		oldPools[poolName] = pool
	}

	newState := &kvEndpointClientManagerState{
		ClientPools: make(map[string]*kvEndpointClientManagerPool),
	}

	for endpoint, endpointConfig := range config.Clients {
		var pool KvClientPool

		oldPool := oldPools[endpoint]
		if oldPool != nil {
			oldPool.Pool.Reconfigure(endpointConfig.KvClientPoolConfig)

			pool = oldPool.Pool
			delete(oldPools, endpoint)
		} else {
			newPool, err := m.newKvClientPool(&KvClientPoolOptions{
				Logger:             m.logger.Named("pool"),
				NumConnections:     m.numPoolConnections,
				KvClientPoolConfig: endpointConfig.KvClientPoolConfig,
			})
			if err != nil {
				m.logger.Debug("failed to create pool", zap.Error(err))
				continue
			}

			pool = newPool
		}

		newState.ClientPools[endpoint] = &kvEndpointClientManagerPool{
			Config: endpointConfig,
			Pool:   pool,
		}
	}

	for _, pool := range oldPools {
		if err := pool.Pool.Close(); err != nil {
			m.logger.Debug("failed to close pool", zap.Error(err))
		}
	}

	m.state.Store(newState)
}

func (m *kvEndpointClientManager) getState() (*kvEndpointClientManagerState, error) {
	state := m.state.Load()
	if state == nil {
		return nil, illegalStateError{"no state data for KvClientManager"}
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
		return pool.Pool.GetClient(ctx)
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

	return pool.Pool.GetClient(ctx)
}

func (m *kvEndpointClientManager) Close() error {
	m.logger.Info("closing kv endpoint client manager")

	m.lock.Lock()
	defer m.lock.Unlock()

	state := m.state.Load()
	if state == nil {
		return nil
	}

	m.state.Store(nil)

	for _, pool := range state.ClientPools {
		if err := pool.Pool.Close(); err != nil {
			m.logger.Debug("Failed to close kv client pool", zap.Error(err))
		}
	}

	if m.onCloseHandler != nil {
		m.onCloseHandler(m)
	}

	m.logger.Info("closed kv endpoint client manager")

	return nil
}
