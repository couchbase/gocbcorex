package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type NewDcpStreamClientProviderFunc func(ctx context.Context, clientOpts *DcpStreamClientOptions) (*DcpStreamClient, error)

type DcpClientManagerClientConfig struct {
	Address       string
	TlsConfig     *tls.Config
	ClientName    string
	Authenticator Authenticator
}

type DcpClientManagerConfig struct {
	Clients map[string]*DcpClientManagerClientConfig
}

type DcpStreamClientManagerOptions struct {
	Logger                 *zap.Logger
	NewDcpClientProviderFn NewDcpStreamClientProviderFunc

	Bucket string

	ConnectionName        string
	ConsumerName          string
	ConnectionFlags       memdx.DcpConnectionFlags
	EnableStreamIds       bool
	NoopInterval          time.Duration
	BufferSize            int
	Priority              string
	ForceValueCompression bool
	EnableCursorDropping  bool
	EnableExpiryEvents    bool
	EnableOso             bool
	EnableSeqNoAdvance    bool
	BackfillOrder         string
	EnableChangeStreams   bool

	Handlers     DcpEventsHandlers
	CloseHandler func(*DcpStreamClient, error)
}

type DcpStreamClientManager struct {
	logger                 *zap.Logger
	newDcpClientProviderFn NewDcpStreamClientProviderFunc

	handlers DcpEventsHandlers

	bucket                string
	connectionName        string
	consumerName          string
	connectionFlags       memdx.DcpConnectionFlags
	enableStreamIds       bool
	noopInterval          time.Duration
	bufferSize            int
	priority              string
	forceValueCompression bool
	enableCursorDropping  bool
	enableExpiryEvents    bool
	enableOso             bool
	enableSeqNoAdvance    bool
	backfillOrder         string
	enableChangeStreams   bool

	lock    sync.Mutex
	clients map[string]*DcpClientManagerClientConfig

	activeClients []*DcpStreamClient
}

func NewDcpStreamClientManager(
	config *DcpClientManagerConfig,
	opts *DcpStreamClientManagerOptions,
) (*DcpStreamClientManager, error) {
	logger := loggerOrNop(opts.Logger)
	// We namespace the pool to improve debugging,
	logger = logger.With(
		zap.String("mgrId", uuid.NewString()[:8]),
	)

	newDcpClientProviderFunc := opts.NewDcpClientProviderFn
	if newDcpClientProviderFunc == nil {
		newDcpClientProviderFunc = NewDcpStreamClient
	}

	mgr := &DcpStreamClientManager{
		logger:                 logger,
		newDcpClientProviderFn: newDcpClientProviderFunc,

		handlers:              opts.Handlers,
		bucket:                opts.Bucket,
		connectionName:        opts.ConnectionName,
		consumerName:          opts.ConsumerName,
		connectionFlags:       opts.ConnectionFlags,
		enableStreamIds:       opts.EnableStreamIds,
		noopInterval:          opts.NoopInterval,
		bufferSize:            opts.BufferSize,
		priority:              opts.Priority,
		forceValueCompression: opts.ForceValueCompression,
		enableCursorDropping:  opts.EnableCursorDropping,
		enableExpiryEvents:    opts.EnableExpiryEvents,
		enableOso:             opts.EnableOso,
		enableSeqNoAdvance:    opts.EnableSeqNoAdvance,
		backfillOrder:         opts.BackfillOrder,
		enableChangeStreams:   opts.EnableChangeStreams,

		clients: config.Clients,
	}

	return mgr, nil
}

func (m *DcpStreamClientManager) Reconfigure(opts *DcpClientManagerConfig) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.clients = opts.Clients

	return nil
}

type DcpClientManagerClientOptions struct {
}

func (m *DcpStreamClientManager) GetClient(
	ctx context.Context,
	endpoint string,
) (*DcpStreamClient, error) {
	m.lock.Lock()
	clientConfig := m.clients[endpoint]
	m.lock.Unlock()

	if clientConfig == nil {
		return nil, errors.New("invalid dcp client endpoint")
	}

	cli, err := m.newDcpClientProviderFn(ctx, &DcpStreamClientOptions{
		Address:       clientConfig.Address,
		TlsConfig:     clientConfig.TlsConfig,
		ClientName:    clientConfig.ClientName,
		Authenticator: clientConfig.Authenticator,
		Bucket:        m.bucket,

		Handlers: m.handlers,

		ConnectionName:        m.connectionName,
		ConsumerName:          m.consumerName,
		ConnectionFlags:       m.connectionFlags,
		NoopInterval:          m.noopInterval,
		BufferSize:            m.bufferSize,
		Priority:              m.priority,
		ForceValueCompression: m.forceValueCompression,
		EnableCursorDropping:  m.enableCursorDropping,
		EnableExpiryEvents:    m.enableExpiryEvents,
		EnableOso:             m.enableOso,
		EnableSeqNoAdvance:    m.enableSeqNoAdvance,
		BackfillOrder:         m.backfillOrder,
		EnableChangeStreams:   m.enableChangeStreams,

		Logger: m.logger.Named("client"),
	})
	if err != nil {
		return nil, err
	}

	m.lock.Lock()
	m.activeClients = append(m.activeClients, cli)
	m.lock.Unlock()

	return cli, nil
}

func (m *DcpStreamClientManager) handleClientClosed(
	client *DcpStreamClient,
	_ error,
) {
	m.lock.Lock()

	clientIdx := slices.IndexFunc(m.activeClients, func(oclient *DcpStreamClient) bool { return oclient == client })
	if clientIdx == -1 {
		return
	}
	m.activeClients = slices.Delete(m.activeClients, clientIdx, clientIdx+1)

	m.lock.Unlock()
}

func (m *DcpStreamClientManager) CloseAllClients() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, client := range m.activeClients {
		err := client.Close()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				continue
			}

			m.logger.Warn("Failed to close dcp client", zap.Error(err))
		}
	}

	return nil
}
