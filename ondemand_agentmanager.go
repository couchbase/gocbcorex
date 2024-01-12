package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

// OnDemandAgentManagerReconfigureOptions is the set of options available for reconfiguring an AgentManager.
type OnDemandAgentManagerReconfigureOptions struct {
	TLSConfig     *tls.Config
	Authenticator Authenticator
}

// OnDemandAgentManagerOptions is the set of options available for creating a new OnDemandAgentManager.
type OnDemandAgentManagerOptions struct {
	Logger *zap.Logger

	TLSConfig          *tls.Config
	Authenticator      Authenticator
	SeedConfig         SeedConfig
	CompressionConfig  CompressionConfig
	ConfigPollerConfig ConfigPollerConfig
	HTTPConfig         HTTPConfig
}

// OnDemandAgentManager is responsible for managing a collection of Agent instances.
type OnDemandAgentManager struct {
	opts         OnDemandAgentManagerOptions
	clusterAgent *Agent

	fastMap  atomic.Pointer[map[string]*Agent]
	slowMap  map[string]*Agent
	slowLock sync.Mutex

	stateLock sync.Mutex
	closed    bool
}

// CreateOnDemandAgentManager creates a new OnDemandAgentManager.
func CreateOnDemandAgentManager(ctx context.Context, opts OnDemandAgentManagerOptions) (*OnDemandAgentManager, error) {
	m := &OnDemandAgentManager{
		opts: opts,

		slowMap: make(map[string]*Agent),
	}

	clusterAgent, err := m.makeAgent(ctx, "")
	if err != nil {
		return nil, err
	}

	m.clusterAgent = clusterAgent
	return m, nil
}

func (m *OnDemandAgentManager) makeAgent(ctx context.Context, bucketName string) (*Agent, error) {
	return CreateAgent(ctx, AgentOptions{
		Logger:             m.opts.Logger,
		TLSConfig:          m.opts.TLSConfig,
		Authenticator:      m.opts.Authenticator,
		SeedConfig:         m.opts.SeedConfig,
		CompressionConfig:  m.opts.CompressionConfig,
		ConfigPollerConfig: m.opts.ConfigPollerConfig,
		HTTPConfig:         m.opts.HTTPConfig,
		BucketName:         bucketName,
	})
}

// Reconfigure reconfigures the AgentManager, and underling Agent instances.
func (m *OnDemandAgentManager) Reconfigure(opts OnDemandAgentManagerReconfigureOptions) error {
	return errors.New("not yet supported")
}

// GetClusterAgent returns the Agent which is not bound to any bucket.
func (m *OnDemandAgentManager) GetClusterAgent() (*Agent, error) {
	m.stateLock.Lock()
	if m.closed {
		m.stateLock.Unlock()
		return nil, errors.New("agent manager closed")
	}
	m.stateLock.Unlock()

	return m.clusterAgent, nil
}

// GetBucketAgent returns the Agent bound to a specific bucket, creating a new Agent as required.
func (m *OnDemandAgentManager) GetBucketAgent(ctx context.Context, bucketName string) (*Agent, error) {
	m.stateLock.Lock()
	if m.closed {
		m.stateLock.Unlock()
		return nil, errors.New("agent manager closed")
	}
	m.stateLock.Unlock()

	fastMap := m.fastMap.Load()
	if fastMap != nil {
		agent, ok := (*fastMap)[bucketName]
		if ok {
			return agent, nil
		}
	}

	agent, err := m.getBucketAgentSlow(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return agent, nil
}

func (m *OnDemandAgentManager) getBucketAgentSlow(ctx context.Context, bucketName string) (*Agent, error) {
	m.slowLock.Lock()
	defer m.slowLock.Unlock()
	if agent, ok := m.slowMap[bucketName]; ok {
		return agent, nil
	}

	agent, err := m.makeAgent(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	m.slowMap[bucketName] = agent

	fastMap := make(map[string]*Agent, len(m.slowMap))
	for name, agent := range m.slowMap {
		fastMap[name] = agent
	}

	m.fastMap.Store(&fastMap)

	return agent, nil
}

// Close closes the AgentManager and all underlying Agent instances.
func (m *OnDemandAgentManager) Close() error {
	m.stateLock.Lock()

	m.opts.Logger.Debug("Closing")

	m.closed = true
	m.fastMap.Store(nil)

	firstErr := m.clusterAgent.Close()

	m.slowLock.Lock()
	agents := m.slowMap
	m.slowMap = make(map[string]*Agent)
	m.slowLock.Unlock()

	m.clusterAgent = nil
	m.stateLock.Unlock()

	for _, agent := range agents {
		err := agent.Close()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	m.opts.Logger.Debug("Closed")

	return firstErr
}
