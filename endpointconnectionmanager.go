package core

import (
	"context"
	"crypto/tls"
	"errors"
	"golang.org/x/exp/slices"
	"strings"
)

type EndpointConnectionProvider interface {
	ShutdownClient(endpoint string, client KvClient)
	GetClient(ctx context.Context, endpoint string) (KvClient, error)
	Reconfigure(opts *EndpointConnectionManagerOptions) error
}

type EndpointConnectionManagerOptions struct {
	NewConnectionProvider func(endpoint string) (ConnectionProvider, error)
	Endpoints             []string
	TLSConfig             *tls.Config
	SelectedBucket        string
	Username              string
	Password              string
}

type EndpointConnectionManager struct {
	config                EndpointConnectionManagerOptions
	newConnectionProvider func(endpoint string) (ConnectionProvider, error)
	endpoints             AtomicPointer[map[string]ConnectionProvider]
}

var _ (EndpointConnectionProvider) = (*EndpointConnectionManager)(nil)

func NewEndpointConnectionManager(opts *EndpointConnectionManagerOptions) (*EndpointConnectionManager, error) {
	if opts == nil {
		return nil, errors.New("must pass options")
	}

	config := *opts

	mgr := &EndpointConnectionManager{
		config: config,
	}

	if config.NewConnectionProvider == nil {
		config.NewConnectionProvider = func(endpoint string) (ConnectionProvider, error) {
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

	endpoints := make(map[string]ConnectionProvider)
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

func (m *EndpointConnectionManager) Reconfigure(opts *EndpointConnectionManagerOptions) error {
	if opts == nil {
		// Nothing we can do here
		return nil
	}

	m.config = *opts
	endpointsPtr := m.endpoints.Load()
	if endpointsPtr == nil {
		return nil
	}

	endpoints := *endpointsPtr
	for _, ep := range m.config.Endpoints {
		_, ok := endpoints[ep]
		if !ok {
			var err error
			hostname := trimSchemePrefix(ep)
			endpoints[ep], err = m.newConnectionProvider(hostname)
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

	return nil
}

func (m *EndpointConnectionManager) GetEndpoint(endpoint string) (ConnectionProvider, error) {
	endpointsPtr := m.endpoints.Load()
	if endpointsPtr == nil {
		return nil, placeholderError{"no endpoints known, shutdown?"}
	}

	endpoints := *endpointsPtr

	if endpoint == "" {
		// Just pick one at random for now
		for _, pool := range endpoints {
			return pool, nil
		}
	}

	pool, ok := endpoints[endpoint]
	if !ok {
		return nil, placeholderError{"endpoint not known"}
	}

	return pool, nil
}

func (m *EndpointConnectionManager) ShutdownClient(endpoint string, client KvClient) {
	connProvider, err := m.GetEndpoint(endpoint)
	if err != nil {
		return
	}

	connProvider.ShutdownClient(client)
}

func (m *EndpointConnectionManager) GetClient(ctx context.Context, endpoint string) (KvClient, error) {
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
