package gocbcorex

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

type MultiKvEndpointClientManager interface {
	NewManager(opts NewManagerOptions) (KvEndpointClientManager, error)
	UpdateEndpoints(endpoints map[string]KvTarget, addOnly bool) error
	Close() error
}

type MultiKvEndpointClientManagerOptions struct {
	Logger *zap.Logger

	Endpoints map[string]KvTarget
	Auth      KvClientAuth
}

type multiKvEndpointClientManager struct {
	logger    *zap.Logger
	lock      sync.Mutex
	endpoints map[string]KvTarget
	auth      KvClientAuth
	managers  []KvEndpointClientManager
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
		logger:    logger,
		endpoints: opts.Endpoints,
		auth:      opts.Auth,
		managers:  make([]KvEndpointClientManager, 0),
	}

	return mgr, nil
}

type NewManagerOptions struct {
	NumPoolConnections       uint
	OnDemandConnect          bool
	ConnectTimeout           time.Duration
	ConnectErrThrottlePeriod time.Duration

	SelectedBucket string
	BootstrapOpts  KvClientBootstrapOptions
	DcpOpts        *KvClientDcpOptions
	DcpHandlers    KvClientDcpEventsHandlers
}

func (m *multiKvEndpointClientManager) NewManager(opts NewManagerOptions) (KvEndpointClientManager, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	mgr, err := NewKvEndpointClientManager(&KvEndpointClientManagerOptions{
		Logger:         m.logger,
		OnCloseHandler: m.handleManagerClosed,

		NumPoolConnections:       opts.NumPoolConnections,
		OnDemandConnect:          opts.OnDemandConnect,
		ConnectTimeout:           opts.ConnectTimeout,
		ConnectErrThrottlePeriod: opts.ConnectErrThrottlePeriod,
		BootstrapOpts:            opts.BootstrapOpts,
		DcpOpts:                  opts.DcpOpts,
		DcpHandlers:              opts.DcpHandlers,

		Endpoints:      m.endpoints,
		Auth:           m.auth,
		SelectedBucket: opts.SelectedBucket,
	})
	if err != nil {
		return nil, err
	}

	m.managers = append(m.managers, mgr)

	return mgr, nil
}

func (m *multiKvEndpointClientManager) UpdateEndpoints(endpoints map[string]KvTarget, addOnly bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.endpoints = endpoints

	for _, manager := range m.managers {
		manager.UpdateEndpoints(endpoints, addOnly)
	}

	return nil
}

func (m *multiKvEndpointClientManager) UpdateAuth(newAuth KvClientAuth) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.auth = newAuth

	for _, manager := range m.managers {
		manager.UpdateAuth(newAuth)
	}
}

func (m *multiKvEndpointClientManager) Close() error {
	m.lock.Lock()
	managers := m.managers
	m.managers = nil
	m.lock.Unlock()

	var firstErr error
	for _, manager := range managers {
		closeErr := manager.Close()
		if closeErr != nil {
			m.logger.Debug("failed to close manager", zap.Error(closeErr))
			if firstErr == nil {
				firstErr = closeErr
			}
		}
	}

	return firstErr
}

func (m *multiKvEndpointClientManager) handleManagerClosed(manager KvEndpointClientManager) {
	m.lock.Lock()
	defer m.lock.Unlock()

	newManagers := make([]KvEndpointClientManager, 0, len(m.managers))

	for _, mgr := range m.managers {
		if mgr != manager {
			newManagers = append(newManagers, mgr)
		}
	}

	m.managers = newManagers
}
