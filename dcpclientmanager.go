package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type DcpClientManagerClientConfig struct {
	Address       string
	TlsConfig     *tls.Config
	Authenticator Authenticator
	ClientName    string
}

type DcpClientManagerConfig struct {
	Clients map[string]*DcpClientManagerClientConfig
}

type NewDcpClientFunc func(clientOpts *DcpClientOptions) (*DcpClient, error)

type DcpClientManagerOptions struct {
	Logger         *zap.Logger
	NewDcpClientFn NewDcpClientFunc
	BucketName     string
	StreamOptions  DcpStreamOptions
	Handlers       DcpClientEventsHandlers
}

type DcpClientManager interface {
	GetClient(ctx context.Context, endpoint string) (*DcpClient, error)
	Reconfigure(opts *DcpClientManagerConfig, cb func(error)) error
	Close() error
}

type dcpClientManagerState struct {
	clients map[string]*DcpClient
	configs map[string]*DcpClientManagerClientConfig
}

type dcpClientManager struct {
	logger         *zap.Logger
	newDcpClientFn NewDcpClientFunc
	bucketName     string
	streamOptions  DcpStreamOptions
	handlers       DcpClientEventsHandlers

	lock  sync.Mutex
	state dcpClientManagerState
}

var _ DcpClientManager = (*dcpClientManager)(nil)

func NewDcpClientManager(
	config *DcpClientManagerConfig,
	opts *DcpClientManagerOptions,
) (DcpClientManager, error) {
	if config == nil {
		return nil, errors.New("must pass config for KvClientManager")
	}
	if opts == nil {
		opts = &DcpClientManagerOptions{}
	}

	logger := loggerOrNop(opts.Logger)
	// We namespace the pool to improve debugging,
	logger = logger.With(
		zap.String("mgrId", uuid.NewString()[:8]),
	)

	mgr := &dcpClientManager{
		logger:         logger,
		newDcpClientFn: opts.NewDcpClientFn,
		bucketName:     opts.BucketName,
		streamOptions:  opts.StreamOptions,
		handlers:       opts.Handlers,

		state: dcpClientManagerState{
			clients: make(map[string]*DcpClient),
		},
	}

	err := mgr.Reconfigure(config, func(error) {})
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

func (m *dcpClientManager) newDcpClient(ctx context.Context, clientOpts *DcpClientOptions) (*DcpClient, error) {
	if m.newDcpClientFn != nil {
		return m.newDcpClientFn(clientOpts)
	}
	return NewDcpClient(ctx, clientOpts)
}

func (m *dcpClientManager) GetClient(ctx context.Context, endpoint string) (*DcpClient, error) {
	// TODO(brett19): Allow multiple concurrent clients to be built at once...

	m.lock.Lock()
	cli := m.state.clients[endpoint]
	if cli == nil {
		config := m.state.configs[endpoint]
		if config == nil {
			m.lock.Unlock()

			var validKeys []string
			for validEndpoint := range m.state.clients {
				validKeys = append(validKeys, validEndpoint)
			}
			return nil, placeholderError{fmt.Sprintf("endpoint not known `%s` in %+v", endpoint, validKeys)}
		}

		newCli, err := m.newDcpClient(ctx, &DcpClientOptions{
			Address:       config.Address,
			TlsConfig:     config.TlsConfig,
			ClientName:    config.ClientName,
			Authenticator: config.Authenticator,
			Bucket:        m.bucketName,

			Handlers: m.handlers,

			ConnectionName:        m.streamOptions.ConnectionName,
			ConsumerName:          m.streamOptions.ConsumerName,
			ConnectionFlags:       memdx.DcpConnectionFlagsProducer,
			NoopInterval:          m.streamOptions.NoopInterval,
			Priority:              m.streamOptions.Priority,
			ForceValueCompression: m.streamOptions.ForceValueCompression,
			EnableExpiryEvents:    m.streamOptions.EnableExpiryEvents,
			EnableStreamIds:       m.streamOptions.EnableStreamIds,
			EnableOso:             m.streamOptions.EnableOso,
			EnableSeqNoAdvance:    m.streamOptions.EnableSeqNoAdvance,
			BackfillOrder:         m.streamOptions.BackfillOrder,
			EnableChangeStreams:   m.streamOptions.EnableChangeStreams,

			Logger:         m.logger,
			NewMemdxClient: nil,
			CloseHandler: func(*DcpClient, error) {

			},
		})
		if err != nil {
			m.lock.Unlock()
			return nil, err
		}

		m.state.clients[endpoint] = newCli
		cli = newCli
	}
	m.lock.Unlock()

	return cli, nil
}

// Reconfigure changes the information about what nodes you connect to.  Note that this does
// not force the clients to be recycled - it is up to the caller to use a brand new
// DcpClientManager if they wish to force all connections to close and reconnect with the
// new tls configuration or authenticator.
func (m *dcpClientManager) Reconfigure(opts *DcpClientManagerConfig, cb func(error)) error {
	m.lock.Lock()
	m.state.configs = opts.Clients
	m.lock.Unlock()

	cb(nil)
	return nil
}

func (m *dcpClientManager) Close() error {
	m.lock.Lock()
	clients := m.state.clients
	m.state.clients = make(map[string]*DcpClient)
	m.lock.Unlock()

	var lastErr error
	for _, client := range clients {
		// TODO(brett19): Handle this error better...
		lastErr = client.Close()
	}

	return lastErr
}
