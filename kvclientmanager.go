package core

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"

	"golang.org/x/exp/slices"
)

type KvClientManagerOptions struct {
	NewConnectionProvider func(endpoint string) (KvClientProvider, error)
	Endpoints             []string
	TLSConfig             *tls.Config
	SelectedBucket        string
	Username              string
	Password              string
}

type KvClientManager struct {
	config                KvClientManagerOptions
	newConnectionProvider func(endpoint string) (KvClientProvider, error)
	endpoints             AtomicPointer[map[string]KvClientProvider]
}

var _ (NodeKvClientProvider) = (*KvClientManager)(nil)

func NewKvClientManager(opts *KvClientManagerOptions) (*KvClientManager, error) {
	if opts == nil {
		return nil, errors.New("must pass options")
	}

	config := *opts

	mgr := &KvClientManager{
		config: config,
	}

	if config.NewConnectionProvider == nil {
		config.NewConnectionProvider = func(endpoint string) (KvClientProvider, error) {
			hostname := trimSchemePrefix(endpoint)
			return NewKvClientPool(&KvClientPoolOptions{
				NewKvClient:    nil,
				NumConnections: 1,
				ClientOpts: KvClientOptions{
					Address:        hostname,
					TlsConfig:      mgr.config.TLSConfig,
					SelectedBucket: mgr.config.SelectedBucket,
					Username:       mgr.config.Username,
					Password:       mgr.config.Password,
				},
			})
		}
	}
	mgr.newConnectionProvider = config.NewConnectionProvider

	endpoints := make(map[string]KvClientProvider)
	for _, ep := range config.Endpoints {
		var err error
		endpoints[ep], err = mgr.newConnectionProvider(ep)
		if err != nil {
			// TODO: Would need to tidy up here.
			return nil, err
		}
	}
	mgr.endpoints.Store(&endpoints)

	return mgr, nil
}

func (m *KvClientManager) Reconfigure(opts *KvClientManagerOptions) error {
	if opts == nil {
		// Nothing we can do here
		return nil
	}

	m.config = *opts

	// TODO: Need to think about whether Reconfigure is going to be called concurrently.
	endpointsPtr := m.endpoints.Load()
	if endpointsPtr == nil {
		return nil
	}

	newEndpoints := make(map[string]KvClientProvider)
	endpoints := *endpointsPtr
	for _, ep := range m.config.Endpoints {
		pool, ok := endpoints[ep]
		if ok {
			newEndpoints[ep] = pool
		} else {
			var err error
			hostname := trimSchemePrefix(ep)
			newEndpoints[ep], err = m.newConnectionProvider(hostname)
			if err != nil {
				// TODO: Would need to tidy up here.
				return err
			}
		}
	}

	for ep, _ := range endpoints {
		if !slices.Contains(m.config.Endpoints, ep) {
			// TODO: Shutdown the pool I guess?
		}
	}

	m.endpoints.Store(&newEndpoints)

	return nil
}

func (m *KvClientManager) GetRandomEndpoint() (KvClientProvider, error) {
	endpointsPtr := m.endpoints.Load()
	if endpointsPtr == nil {
		return nil, placeholderError{"no endpoints known, shutdown?"}
	}

	endpoints := *endpointsPtr

	// Just pick one at random for now
	for _, pool := range endpoints {
		return pool, nil
	}

	return nil, placeholderError{"no endpoints known, shutdown?"}
}

func (m *KvClientManager) GetEndpoint(endpoint string) (KvClientProvider, error) {
	if endpoint == "" {
		return nil, placeholderError{"endpoint must be specified for GetEndpoint"}
	}

	endpointsPtr := m.endpoints.Load()
	if endpointsPtr == nil {
		return nil, placeholderError{"no endpoints known, shutdown?"}
	}

	endpoints := *endpointsPtr

	pool, ok := endpoints[endpoint]
	if !ok {
		return nil, placeholderError{"endpoint not known"}
	}

	return pool, nil
}

func (m *KvClientManager) shutdownRandomClient(client KvClient) {
	endpointsPtr := m.endpoints.Load()
	if endpointsPtr == nil {
		return
	}

	endpoints := *endpointsPtr

	for _, endpoint := range endpoints {
		endpoint.ShutdownClient(client)
	}
}

func (m *KvClientManager) ShutdownClient(endpoint string, client KvClient) {
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

func (m *KvClientManager) GetRandomClient(ctx context.Context) (KvClient, error) {
	connProvider, err := m.GetRandomEndpoint()
	if err != nil {
		return nil, err
	}

	return connProvider.GetClient(ctx)
}

func (m *KvClientManager) GetClient(ctx context.Context, endpoint string) (KvClient, error) {
	connProvider, err := m.GetEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	return connProvider.GetClient(ctx)
}

func trimSchemePrefix(address string) string {
	idx := strings.Index(address, "://")
	if idx < 0 {
		return address
	}

	return address[idx+len("://"):]
}
