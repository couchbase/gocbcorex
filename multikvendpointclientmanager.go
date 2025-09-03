package gocbcorex

import (
	"crypto/tls"
	"sync"
	"time"

	"go.uber.org/zap"
)

type MultiKvEndpointClientManager interface {
	NewManager(opts NewManagerOptions) (KvEndpointClientManager, error)
	Reconfigure(newConfig MultiKvEndpointClientManagerConfig)
	Close() error
}

type multiKvEndpointClientManagerEntry struct {
	Manager KvEndpointClientManager
	Options NewManagerOptions
}

type MultiKvEndpointClientManagerConfigClient struct {
	Address       string
	TlsConfig     *tls.Config
	Authenticator Authenticator
}

type MultiKvEndpointClientManagerConfig struct {
	Clients map[string]MultiKvEndpointClientManagerConfigClient
}

type MultiKvEndpointClientManagerOptions struct {
	Logger *zap.Logger

	MultiKvEndpointClientManagerConfig
}

type multiKvEndpointClientManager struct {
	logger        *zap.Logger
	lock          sync.Mutex
	currentConfig *MultiKvEndpointClientManagerConfig
	managers      []*multiKvEndpointClientManagerEntry
}

var _ MultiKvEndpointClientManager = (*multiKvEndpointClientManager)(nil)

func NewMultiKvEndpointClientManager(opts *MultiKvEndpointClientManagerOptions) (MultiKvEndpointClientManager, error) {
	if opts == nil {
		opts = &MultiKvEndpointClientManagerOptions{}
	}
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	mgr := &multiKvEndpointClientManager{
		logger:        logger,
		currentConfig: &opts.MultiKvEndpointClientManagerConfig,
	}

	return mgr, nil
}

type NewManagerOptions struct {
	NumPoolConnections       uint
	OnDemandConnect          bool
	ConnectTimeout           time.Duration
	ConnectErrThrottlePeriod time.Duration

	BucketName    string
	BootstrapOpts KvClientBootstrapOptions
	DcpOpts       *KvClientDcpOptions
	DcpHandlers   *KvClientDcpEventsHandlers
}

func (m *multiKvEndpointClientManager) generateManagerConfigLocked(opts NewManagerOptions) KvEndpointClientManagerConfig {
	clients := make(map[string]KvEndpointClientManagerConfigClient, len(m.currentConfig.Clients))
	for name, client := range m.currentConfig.Clients {
		clients[name] = KvEndpointClientManagerConfigClient{
			KvClientPoolConfig: KvClientPoolConfig{
				KvClientManagerConfig: KvClientManagerConfig{
					Address:        client.Address,
					TlsConfig:      client.TlsConfig,
					Authenticator:  client.Authenticator,
					SelectedBucket: opts.BucketName,
					BootstrapOpts:  opts.BootstrapOpts,
					DcpOpts:        opts.DcpOpts,
				},
			},
		}
	}

	return KvEndpointClientManagerConfig{
		Clients: clients,
	}
}

func (m *multiKvEndpointClientManager) NewManager(opts NewManagerOptions) (KvEndpointClientManager, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	mgrConfig := m.generateManagerConfigLocked(opts)

	mgr, err := NewKvEndpointClientManager(&KvEndpointClientManagerOptions{
		Logger:         m.logger,
		OnCloseHandler: m.handleManagerClosed,

		NumPoolConnections:       opts.NumPoolConnections,
		OnDemandConnect:          opts.OnDemandConnect,
		ConnectTimeout:           opts.ConnectTimeout,
		ConnectErrThrottlePeriod: opts.ConnectErrThrottlePeriod,

		KvEndpointClientManagerConfig: mgrConfig,
	})
	if err != nil {
		return nil, err
	}

	m.managers = append(m.managers, &multiKvEndpointClientManagerEntry{
		Manager: mgr,
		Options: opts,
	})

	return mgr, nil
}

func (m *multiKvEndpointClientManager) Reconfigure(newConfig MultiKvEndpointClientManagerConfig) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.currentConfig = &newConfig

	for _, manager := range m.managers {
		manager.Manager.Reconfigure(
			m.generateManagerConfigLocked(manager.Options))
	}
}

func (m *multiKvEndpointClientManager) Close() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	var firstErr error
	for _, manager := range m.managers {
		closeErr := manager.Manager.Close()
		if closeErr != nil {
			m.logger.Debug("failed to close manager", zap.Error(closeErr))
			if firstErr == nil {
				firstErr = closeErr
			}
		}
	}

	m.managers = nil

	return firstErr
}

func (m *multiKvEndpointClientManager) handleManagerClosed(manager KvEndpointClientManager) {
	m.lock.Lock()
	defer m.lock.Unlock()

	newManagers := make([]*multiKvEndpointClientManagerEntry, 0, len(m.managers)-1)

	for _, mgr := range m.managers {
		if mgr.Manager != manager {
			newManagers = append(newManagers, mgr)
		}
	}

	m.managers = newManagers
}
