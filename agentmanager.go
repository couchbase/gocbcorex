package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"

	"go.uber.org/zap"
)

type AgentManagerReconfigureOptions struct {
	TLSConfig     *tls.Config
	Authenticator Authenticator
}

type AgentManagerOptions struct {
	Logger *zap.Logger

	TLSConfig          *tls.Config
	Authenticator      Authenticator
	SeedConfig         SeedConfig
	CompressionConfig  CompressionConfig
	ConfigPollerConfig ConfigPollerConfig
	HTTPConfig         HTTPConfig
}

type AgentManager struct {
	lock         sync.Mutex
	opts         AgentManagerOptions
	clusterAgent *Agent
	bucketAgents map[string]*Agent
}

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

func (m *AgentManager) Reconfigure(opts AgentManagerReconfigureOptions) error {
	return errors.New("not yet supported")
}

// TODO(brett19): This should return a ClusterAgent
func (m *AgentManager) GetClusterAgent() *Agent {
	return m.clusterAgent
}

// TODO(brett19): This should return a BucketAgent
func (m *AgentManager) GetBucketAgent(ctx context.Context, bucketName string) (*Agent, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	// TODO(brett19): This shouldn't block everyone every time a new bucket needs to connect....

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
