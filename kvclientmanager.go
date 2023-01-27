package core

import (
	"context"
	"errors"
	"fmt"
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
	Clients map[string]*KvClientPoolConfig
}

type KvClientManagerOptions struct {
	NewKvClientProviderFn NewKvClientProviderFunc
}

type kvClientManagerState struct {
	ClientPools map[string]KvClientPool
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

	if len(state.ClientPools) != 0 {
		// TODO(brett19): add reconfigure support
		return errors.New("reconfiguring the KvClientManager is not currently supported")
	}

	newState := &kvClientManagerState{
		ClientPools: make(map[string]KvClientPool),
	}

	for endpoint, endpointConfig := range config.Clients {
		clientPool, err := m.newKvClientProvider(endpointConfig)
		if err != nil {
			return err
		}

		newState.ClientPools[endpoint] = clientPool
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
		return pool, nil
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

	return pool, nil
}

func (m *kvClientManager) shutdownRandomClient(client KvClient) {
	state, err := m.getState()
	if err != nil {
		return
	}

	for _, endpoint := range state.ClientPools {
		endpoint.ShutdownClient(client)
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
