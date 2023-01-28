package core

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
)

var (
	ErrNoClientsAvailable = errors.New("no clients available")
	ErrInvalidClient      = errors.New("invalid client requested")
)

type KvClientManager interface {
	ShutdownClient(endpoint string, client KvClient)
	GetClient(ctx context.Context, endpoint string) (KvClient, error)
	Reconfigure(opts *KvClientManagerConfig) error
	GetRandomClient(ctx context.Context) (KvClient, error)
}

type NewKvClientProviderFunc func(clientOpts *KvClientPoolConfig) (KvClientPool, error)

type KvClientManagerConfig struct {
	NumPoolConnections uint
	Clients            map[string]*KvClientConfig
}

type KvClientManagerOptions struct {
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
	newKvClientProviderFn NewKvClientProviderFunc

	lock          sync.Mutex
	currentConfig KvClientManagerConfig
	state         AtomicPointer[kvClientManagerState]
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

	mgr := &kvClientManager{
		newKvClientProviderFn: opts.NewKvClientProviderFn,
	}

	// initialize the client manager to having no clients
	mgr.currentConfig = KvClientManagerConfig{}
	mgr.state.Store(&kvClientManagerState{})

	err := mgr.Reconfigure(config)
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

func (m *kvClientManager) newKvClientProvider(poolOpts *KvClientPoolConfig) (KvClientPool, error) {
	if m.newKvClientProviderFn != nil {
		return m.newKvClientProviderFn(poolOpts)
	}
	return NewKvClientPool(poolOpts, nil)
}

func (m *kvClientManager) Reconfigure(config *KvClientManagerConfig) error {
	if config == nil {
		return errors.New("invalid arguments: cant reconfigure kvClientManager to nil")
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	state := m.state.Load()
	if state == nil {
		return illegalStateError{"KvClientManager reconfigure expected state"}
	}

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
			ClientOpts:     *endpointConfig,
		}

		var pool KvClientPool

		oldPool := oldPools[endpoint]
		if oldPool != nil {
			err := oldPool.Pool.Reconfigure(poolConfig)
			if err != nil {
				log.Printf("failed to reconfigure pool: %s", err)
			} else {
				pool = oldPool.Pool
			}
			delete(oldPools, endpoint)
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

	// TODO(brett19): Clean up the pools that were destroyed.
	// we will need to keep track of them to support doing a full shut
	// down, similar to how KvClientPool handles it.

	// TODO(brett19): optimize this to avoid recreating pools
	// right now, if the name of the pool changes, it causes a new pool
	// to be created and the old one destroyed.  We should try to match
	// any dead pools to the new pools being created, and reuse them
	// instead.

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
			var dispatchErr KvClientDispatchError
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
			var dispatchErr KvClientDispatchError
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
