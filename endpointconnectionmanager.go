package core

import (
	"context"
	"crypto/tls"
)

type EndpointConnectionProvider interface {
	ShutdownClient(endpoint string, client KvClient)
	GetClient(ctx context.Context, endpoint string) (KvClient, error)
}

type EndpointConnectionManagerOptions struct {
	NewConnectionProvider func() (ConnectionProvider, error)
	Endpoints             map[string]string
	TLSConfig             *tls.Config
}

type EndpointConnectionManager struct {
	config EndpointConnectionManagerOptions
}

var _ (EndpointConnectionProvider) = (*EndpointConnectionManager)(nil)

func NewEndpointConnectionManager() (*EndpointConnectionManager, error) {
	return nil, nil
}

func (m *EndpointConnectionManager) Reconfigure(opts *EndpointConnectionManagerOptions) error {
	return nil
}

func (m *EndpointConnectionManager) GetEndpoint(endpoint string) (ConnectionProvider, error) {
	return nil, nil
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
