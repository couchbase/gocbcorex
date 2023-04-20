package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"

	"go.uber.org/zap"
)

// AgentManagerReconfigureOptions is the set of options available for reconfiguring an AgentManager.
type AgentManagerReconfigureOptions struct {
	TLSConfig     *tls.Config
	Authenticator Authenticator
}

// AgentManagerOptions is the set of options available for creating a new AgentManager.
type AgentManagerOptions struct {
	Logger *zap.Logger

	TLSConfig          *tls.Config
	Authenticator      Authenticator
	SeedConfig         SeedConfig
	CompressionConfig  CompressionConfig
	ConfigPollerConfig ConfigPollerConfig
	HTTPConfig         HTTPConfig
}

// AgentManager is responsible for managing a collection of Agent instances.
type AgentManager struct {
	lock         sync.Mutex
	opts         AgentManagerOptions
	clusterAgent *Agent
	bucketAgents map[string]*Agent

	closed bool
}

// CreateAgentManager creates a new AgentManager.
func CreateAgentManager(ctx context.Context, opts AgentManagerOptions) (*AgentManager, error) {
	m := &AgentManager{
		opts: opts,
	}

	clusterAgent, err := m.makeAgentLocked(ctx, "")
	if err != nil {
		return nil, err
	}

	m.clusterAgent = clusterAgent
	return m, nil
}

func (m *AgentManager) makeAgentLocked(ctx context.Context, bucketName string) (*Agent, error) {
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
func (m *AgentManager) Reconfigure(opts AgentManagerReconfigureOptions) error {
	return errors.New("not yet supported")
}

// GetClusterAgent returns the Agent which is not bound to any bucket.
func (m *AgentManager) GetClusterAgent() (*Agent, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.closed {
		return nil, errors.New("agent manager closed")
	}

	return m.clusterAgent, nil
}

// GetBucketAgent returns the Agent bound to a specific bucket, creating a new Agent as required.
func (m *AgentManager) GetBucketAgent(ctx context.Context, bucketName string) (*Agent, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.closed {
		return nil, errors.New("agent manager closed")
	}

	if m.bucketAgents == nil {
		m.bucketAgents = make(map[string]*Agent)
	} else {
		bucketAgent, ok := m.bucketAgents[bucketName]
		if ok {
			return bucketAgent, nil
		}
	}

	bucketAgent, err := m.makeAgentLocked(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	m.bucketAgents[bucketName] = bucketAgent

	return bucketAgent, nil
}

// Close closes the AgentManager and all underlying Agent instances.
func (m *AgentManager) Close() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	firstErr := m.clusterAgent.Close()
	for _, agent := range m.bucketAgents {
		err := agent.Close()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	m.clusterAgent = nil
	m.bucketAgents = nil
	m.closed = true

	return firstErr
}
