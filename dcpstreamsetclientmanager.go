package gocbcorex

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

type DcpStreamSetClientManager interface {
	NewClientManager(bucketName string) (DcpEndpointClientManager, error)
	Reconfigure(newConfig DcpStreamSetClientManagerConfig)
	Close() error
}

type dcpStreamSetClientManager struct {
	logger *zap.Logger

	lock          sync.Mutex
	currentConfig DcpStreamSetClientManagerConfig
	managers      []DcpEndpointClientManager
}

var _ DcpStreamSetClientManager = (*dcpStreamSetClientManager)(nil)

type DcpStreamSetClientManagerConfig struct {
	DcpEndpointClientManagerConfig
}

type DcpStreamSetClientManagerOptions struct {
	Logger          *zap.Logger
	NewKvClientPool NewKvClientPoolFunc

	OnDemandConnect          bool
	ConnectTimeout           time.Duration
	ConnectErrThrottlePeriod time.Duration

	DcpStreamSetClientManagerConfig
}

func NewDcpStreamSetClientManager(opts *DcpStreamSetClientManagerOptions) (DcpStreamSetClientManager, error) {
	m := &dcpStreamSetClientManager{
		logger: opts.Logger,

		currentConfig: DcpStreamSetClientManagerConfig{
			DcpEndpointClientManagerConfig: opts.DcpStreamSetClientManagerConfig.DcpEndpointClientManagerConfig,
		},
		managers: make([]DcpEndpointClientManager, 0),
	}

	return m, nil
}

func (m *dcpStreamSetClientManager) NewClientManager(bucketName string) (DcpEndpointClientManager, error) {
	mgr, err := NewDcpEndpointClientManager(&DcpEndpointClientManagerOptions{
		Logger:                   m.logger,
		BucketName:               bucketName,
		OnDemandConnect:          true,
		ConnectTimeout:           5 * time.Second,
		ConnectErrThrottlePeriod: time.Minute,
		CloseHandler:             m.handleManagerClose,
		DcpEndpointClientManagerConfig: DcpEndpointClientManagerConfig{
			Clients: m.currentConfig.Clients,
		},
	})
	if err != nil {
		return nil, err
	}

	m.lock.Lock()
	m.managers = append(m.managers, mgr)
	m.lock.Unlock()

	return mgr, nil
}

func (m *dcpStreamSetClientManager) Reconfigure(newConfig DcpStreamSetClientManagerConfig) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.currentConfig = newConfig

	for _, manager := range m.managers {
		manager.Reconfigure(newConfig.DcpEndpointClientManagerConfig)
	}
}

func (m *dcpStreamSetClientManager) Close() error {
	m.lock.Lock()
	managers := m.managers
	m.managers = nil
	m.lock.Unlock()

	for _, manager := range managers {
		if err := manager.Close(); err != nil {
			m.logger.Warn("failed to close DcpEndpointClientManager", zap.Error(err))
		}
	}

	return nil
}

func (m *dcpStreamSetClientManager) handleManagerClose(manager DcpEndpointClientManager) {
	m.lock.Lock()
	defer m.lock.Unlock()

	newManagers := make([]DcpEndpointClientManager, 0, len(m.managers))
	for _, mgr := range m.managers {
		if mgr != manager {
			newManagers = append(newManagers, mgr)
		}
	}

	m.managers = newManagers
}
