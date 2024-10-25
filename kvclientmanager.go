package gocbcorex

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"go.uber.org/zap"
)

var (
	ErrNoClientsAvailable = errors.New("no clients available")
	ErrInvalidClient      = errors.New("invalid client requested")
)

type KvClientManager interface {
	ShutdownClient(endpoint string, client KvClient)
	GetClient(ctx context.Context, endpoint string) (KvClient, error)
	Reconfigure(opts *KvClientManagerConfig, cb func(error)) error
	GetRandomClient(ctx context.Context) (KvClient, error)
	Close() error
}

type NewKvClientProviderFunc func(clientOpts *KvClientPoolConfig) (KvClientPool, error)

type KvClientManagerConfig struct {
	NumPoolConnections uint
	Clients            map[string]*KvClientConfig
}

type KvClientManagerOptions struct {
	Logger                *zap.Logger
	NewKvClientProviderFn NewKvClientProviderFunc
}

type kvClientManagerPool struct {
	Config *KvClientPoolConfig
	Pool   KvClientPool
}

type kvClientManagerState struct {
	ClientPools map[string]*kvClientManagerPool
}

type kvClientManager struct {
	logger                *zap.Logger
	newKvClientProviderFn NewKvClientProviderFunc

	lock          sync.Mutex
	currentConfig KvClientManagerConfig
	state         atomic.Pointer[kvClientManagerState]
}

var _ (KvClientManager) = (*kvClientManager)(nil)

func NewKvClientManager(
	config *KvClientManagerConfig,
	opts *KvClientManagerOptions,
) (*kvClientManager, error) {
	if config == nil {
		return nil, errors.New("must pass config for KvClientManager")
	}
	if opts == nil {
		opts = &KvClientManagerOptions{}
	}

	logger := loggerOrNop(opts.Logger)
	// We namespace the pool to improve debugging,
	logger = logger.With(
		zap.String("mgrId", uuid.NewString()[:8]),
	)

	mgr := &kvClientManager{
		logger:                logger,
		newKvClientProviderFn: opts.NewKvClientProviderFn,
	}

	// initialize the client manager to having no clients
	mgr.currentConfig = KvClientManagerConfig{}
	mgr.state.Store(&kvClientManagerState{})

	// we just call Reconfigure.  Since we know there are no pools that
	// exist, we know that Reconfigure is guarenteed not to block.
	err := mgr.Reconfigure(config, func(error) {})
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

func (m *kvClientManager) newKvClientProvider(poolOpts *KvClientPoolConfig) (KvClientPool, error) {
	if m.newKvClientProviderFn != nil {
		return m.newKvClientProviderFn(poolOpts)
	}
	return NewKvClientPool(poolOpts, &KvClientPoolOptions{
		Logger: m.logger.Named("pool"),
	})
}

func (m *kvClientManager) Reconfigure(config *KvClientManagerConfig, cb func(error)) error {
	if config == nil {
		return errors.New("invalid arguments: cant reconfigure kvClientManager to nil")
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	state := m.state.Load()
	if state == nil {
		return illegalStateError{"kvClientManager reconfigure expected state"}
	}

	m.logger.Debug("reconfiguring")

	oldPools := make(map[string]*kvClientManagerPool)
	for poolName, pool := range state.ClientPools {
		oldPools[poolName] = pool
	}

	newState := &kvClientManagerState{
		ClientPools: make(map[string]*kvClientManagerPool),
	}

	for endpoint, endpointConfig := range config.Clients {
		poolConfig := &KvClientPoolConfig{
			NumConnections: config.NumPoolConnections,
			ClientConfig:   *endpointConfig,
		}

		var pool KvClientPool

		oldPool := oldPools[endpoint]
		if oldPool != nil {
			err := oldPool.Pool.Reconfigure(poolConfig, func(error) {})
			if err != nil {
				m.logger.Debug("failed to reconfigure pool", zap.Error(err))
			} else {
				pool = oldPool.Pool
				delete(oldPools, endpoint)
			}
		}

		if pool == nil {
			newPool, err := m.newKvClientProvider(poolConfig)
			if err != nil {
				return err
			}

			pool = newPool
		}

		newState.ClientPools[endpoint] = &kvClientManagerPool{
			Config: poolConfig,
			Pool:   pool,
		}
	}

	for _, pool := range oldPools {
		if err := pool.Pool.Close(); err != nil {
			m.logger.Debug("failed to close pool", zap.Error(err))
		}
	}

	m.state.Store(newState)

	return nil
}

func (m *kvClientManager) getState() (*kvClientManagerState, error) {
	state := m.state.Load()
	if state == nil {
		return nil, illegalStateError{"no state data for KvClientManager"}
	}

	return state, nil
}

func (m *kvClientManager) GetRandomEndpoint() (KvClientPool, error) {
	state, err := m.getState()
	if err != nil {
		return nil, err
	}

	// Just pick one at random for now
	for _, pool := range state.ClientPools {
		return pool.Pool, nil
	}

	return nil, placeholderError{"no endpoints known, shutdown?"}
}

func (m *kvClientManager) GetEndpoint(endpoint string) (KvClientPool, error) {
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

	return pool.Pool, nil
}

func (m *kvClientManager) shutdownRandomClient(client KvClient) {
	state, err := m.getState()
	if err != nil {
		return
	}

	for _, endpoint := range state.ClientPools {
		endpoint.Pool.ShutdownClient(client)
	}
}

func (m *kvClientManager) ShutdownClient(endpoint string, client KvClient) {
	if endpoint == "" {
		// we don't know which endpoint this belongs to, so we need to send the
		// shutdown request to all of the possibilities...
		m.shutdownRandomClient(client)
		return
	}

	connProvider, err := m.GetEndpoint(endpoint)
	if err != nil {
		return
	}

	connProvider.ShutdownClient(client)
}

func (m *kvClientManager) GetRandomClient(ctx context.Context) (KvClient, error) {
	connProvider, err := m.GetRandomEndpoint()
	if err != nil {
		return nil, err
	}

	return connProvider.GetClient(ctx)
}

func (m *kvClientManager) GetClient(ctx context.Context, endpoint string) (KvClient, error) {
	connProvider, err := m.GetEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	return connProvider.GetClient(ctx)
}
func (m *kvClientManager) Close() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	state := m.state.Load()
	if state == nil {
		return nil
	}

	m.logger.Info("closing kv client manager")

	m.state.Store(nil)

	for _, pool := range state.ClientPools {
		if err := pool.Pool.Close(); err != nil {
			m.logger.Debug("Failed to close kv client pool", zap.Error(err))
		}

	}

	m.logger.Info("closed kv client manager")

	return nil
}

func OrchestrateMemdClient[RespT any](
	ctx context.Context,
	cm KvClientManager,
	endpoint string,
	fn func(client KvClient) (RespT, error),
) (RespT, error) {
	for {
		cli, err := cm.GetClient(ctx, endpoint)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		res, err := fn(cli)
		if err != nil {
			var dispatchErr *KvClientDispatchError
			if errors.As(err, &dispatchErr) {
				// this was a dispatch error, so we can just try with
				// a different client instead...
				cm.ShutdownClient(endpoint, cli)
				continue
			}

			return res, err
		}

		return res, nil
	}
}

func OrchestrateRandomMemdClient[RespT any](
	ctx context.Context,
	cm KvClientManager,
	fn func(client KvClient) (RespT, error),
) (RespT, error) {
	for {
		cli, err := cm.GetRandomClient(ctx)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		res, err := fn(cli)
		if err != nil {
			var dispatchErr *KvClientDispatchError
			if errors.As(err, &dispatchErr) {
				// this was a dispatch error, so we can just try with
				// a different client instead...
				cm.ShutdownClient("", cli)
				continue
			}

			return res, err
		}

		return res, nil
	}
}
