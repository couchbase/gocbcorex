package core

import (
	"crypto/tls"
	"math/rand"
	"sync"

	"github.com/couchbase/stellar-nebula/core/memdx"

	"golang.org/x/exp/slices"
	"golang.org/x/net/context"
)

type ConnExecuteHandler func(KvClient) error

type ConnectionManager interface {
	UpdateEndpoints(endpoints []string) error
	Execute(endpoint string, handler ConnExecuteHandler) error
}

type connMgrPipeline struct {
	connections []KvClient
}

func (pipeline *connMgrPipeline) Close() error {
	for _, conn := range pipeline.connections {
		conn.Close()
	}

	return nil
}

type connectionManager struct {
	lock        sync.Mutex
	connections map[string]*connMgrPipeline

	connectionsPerNode uint8

	tlsConfig *tls.Config
	bucket    string

	username string
	password string

	requestedFeatures []memdx.HelloFeature
}

var _ ConnectionManager = (*connectionManager)(nil)

type connManagerOptions struct {
	ConnectionsPerNode uint8

	TlsConfig      *tls.Config
	SelectedBucket string
	Features       []memdx.HelloFeature

	// TODO: this should probably be swapped out for an authenticator of some sort, which can probably be hotswapped
	// during Reconfigure.
	Username string
	Password string
}

func newConnectionManager(opts connManagerOptions) *connectionManager {
	return &connectionManager{
		connections:        make(map[string]*connMgrPipeline),
		connectionsPerNode: opts.ConnectionsPerNode,
		tlsConfig:          opts.TlsConfig,
		requestedFeatures:  opts.Features,
		username:           opts.Username,
		password:           opts.Password,
		bucket:             opts.SelectedBucket,
	}
}

func (m *connectionManager) UpdateEndpoints(endpoints []string) error {
	m.lock.Lock()

	for _, endpoint := range endpoints {
		if m.connections[endpoint] == nil {
			clients := make([]KvClient, m.connectionsPerNode)
			for i := uint8(0); i < m.connectionsPerNode; i++ {
				cli, err := newKvClient(context.Background(), &KvClientOptions{
					Address:        endpoint,
					TlsConfig:      m.tlsConfig,
					SelectedBucket: m.bucket,
					//Features:       m.requestedFeatures,
					Username: m.username,
					Password: m.password,
				})

				if err != nil {
					m.lock.Unlock()
					return err // TODO: not really sure what we'd do here...
				}

				/*
					_, err = cli.Bootstrap()
					if err != nil {
						m.lock.Unlock()
						return err // TODO: not really sure what we'd do here...
					}
				*/

				clients[i] = cli
			}
			m.connections[endpoint] = &connMgrPipeline{connections: clients}
		}
	}

	for endpoint, pipeline := range m.connections {
		if !slices.Contains(endpoints, endpoint) {
			pipeline.Close()
		}
	}

	m.lock.Unlock()

	return nil
}

func (m *connectionManager) GetClientForEndpoint(endpoint string) (KvClient, error) {
	return nil, nil
}

func (m *connectionManager) Execute(endpoint string, handler ConnExecuteHandler) error {
	m.lock.Lock()
	pipeline, ok := m.connections[endpoint]
	if !ok {
		m.lock.Unlock()
		return placeholderError{Inner: "endpoint not known"}
	}
	client := pipeline.connections[rand.Intn(len(pipeline.connections))]
	m.lock.Unlock()

	return handler(client)
}
