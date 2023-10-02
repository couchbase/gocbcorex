package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"sync"

	"golang.org/x/exp/slices"

	"go.uber.org/zap"
)

type BucketsTrackingAgentManagerReconfigureOptions struct {
	TLSConfig     *tls.Config
	Authenticator Authenticator
}

type BucketsTrackingAgentManagerOptions struct {
	Logger *zap.Logger

	TLSConfig          *tls.Config
	Authenticator      Authenticator
	SeedConfig         SeedConfig
	CompressionConfig  CompressionConfig
	ConfigPollerConfig ConfigPollerConfig
	HTTPConfig         HTTPConfig
}

type bucketsTrackingAgentManagerState struct {
	tlsConfig     *tls.Config
	authenticator Authenticator
	httpTransport *http.Transport

	latestConfig *ParsedConfig
}

type BucketsTrackingAgentManager struct {
	bucketsLock sync.Mutex
	topoLock    sync.Mutex
	opts        BucketsTrackingAgentManagerOptions
	logger      *zap.Logger
	userAgent   string
	networkType string

	state *bucketsTrackingAgentManagerState

	clusterAgent *Agent
	bucketAgents map[string]*Agent
	closed       bool

	bucketsWatcher  *StreamWatcherHttp[[]bucketDescriptor]
	watchersCancel  func()
	needsBucketChan chan struct{}

	topologyCfgWatcher *ConfigWatcherHttp
}

type bucketDescriptor struct {
	Name string
	UUID string
}

func CreateBucketsTrackingAgentManager(ctx context.Context, opts BucketsTrackingAgentManagerOptions) (*BucketsTrackingAgentManager, error) {
	srcHTTPAddrs := makeSrcHTTPAddrs(opts.SeedConfig.HTTPAddrs, opts.TLSConfig)
	httpTransport := makeHTTPTransport(opts.TLSConfig)
	httpUserAgent := "gocbcorex/0.0.1-dev"

	logger := loggerOrNop(opts.Logger)
	logger = logger.Named("agent manager")

	bootstrapper, err := NewConfigBootstrapHttp(ConfigBoostrapHttpOptions{
		Logger:           logger.Named("http-bootstrap"),
		HttpRoundTripper: httpTransport,
		Endpoints:        srcHTTPAddrs,
		UserAgent:        httpUserAgent,
		Authenticator:    opts.Authenticator,
	})
	if err != nil {
		return nil, err
	}

	bootstrapConfig, networkType, err := bootstrapper.Bootstrap(ctx)
	if err != nil {
		return nil, err
	}

	logger.Debug("internal service agent manager bootstrapped",
		zap.Any("bootstrapConfig", bootstrapConfig),
		zap.String("networkType", networkType))

	m := &BucketsTrackingAgentManager{
		opts:         opts,
		logger:       logger,
		userAgent:    httpUserAgent,
		networkType:  networkType,
		bucketAgents: make(map[string]*Agent),
		state: &bucketsTrackingAgentManagerState{
			tlsConfig:     opts.TLSConfig,
			authenticator: opts.Authenticator,
			httpTransport: httpTransport,
			latestConfig:  bootstrapConfig,
		},
	}

	bucketsWatcher, err := NewStreamWatcherHttp[[]bucketDescriptor](&StreamWatcherHttpConfig{
		HttpRoundTripper: httpTransport,
		Endpoints:        srcHTTPAddrs,
		UserAgent:        httpUserAgent,
		Authenticator:    opts.Authenticator,
	}, &StreamWatcherHttpOptions{
		Logger: logger.Named("buckets-watcher"),
	})
	if err != nil {
		return nil, err
	}

	m.bucketsWatcher = bucketsWatcher

	topoWatcher, err := NewConfigWatcherHttp(
		&ConfigWatcherHttpConfig{
			HttpRoundTripper: httpTransport,
			Endpoints:        srcHTTPAddrs,
			UserAgent:        httpUserAgent,
			Authenticator:    opts.Authenticator,
		},
		&ConfigWatcherHttpOptions{
			Logger: logger.Named("topology-watcher"),
		})
	if err != nil {
		return nil, err
	}

	m.topologyCfgWatcher = topoWatcher

	clusterAgent, err := m.makeAgentLocked(ctx, "")
	if err != nil {
		return nil, err
	}

	m.clusterAgent = clusterAgent

	m.startWatchers()

	return m, nil
}

func (m *BucketsTrackingAgentManager) startWatchers() {
	ctx, cancel := context.WithCancel(context.Background())

	stoppedTopoSig := make(chan struct{})
	stoppedBucketSig := make(chan struct{})
	m.watchersCancel = func() {
		// Cancel the context and wait for the configCh to be closed.
		cancel()
		<-stoppedBucketSig
		<-stoppedTopoSig
	}

	bucketCh := m.bucketsWatcher.Watch(ctx, streamWatcherHttp_streamBuckets)

	go func() {
		for buckets := range bucketCh {
			m.handleBuckets(buckets)
		}
		close(stoppedBucketSig)
	}()

	topoCh := m.topologyCfgWatcher.Watch(ctx)

	go func() {
		for config := range topoCh {
			m.applyConfig(config)
		}
		close(stoppedTopoSig)
	}()
}

func (m *BucketsTrackingAgentManager) applyConfig(config *ParsedConfig) {
	m.topoLock.Lock()
	defer m.topoLock.Unlock()

	if !canUpdateConfig(config, m.state.latestConfig, m.opts.Logger) {
		return
	}

	m.logger.Debug("Applying new config", zap.Int64("revId", config.RevID), zap.Int64("revEpoch", config.RevEpoch))

	m.state.latestConfig = config

	bootstrapHosts := m.state.latestConfig.AddressesGroupForNetworkType(m.networkType)

	var mgmtEndpoints []string
	tlsConfig := m.state.tlsConfig
	if tlsConfig == nil {
		for _, host := range bootstrapHosts.NonSSL.Mgmt {
			mgmtEndpoints = append(mgmtEndpoints, "http://"+host)
		}
	} else {
		for _, host := range bootstrapHosts.SSL.Mgmt {
			mgmtEndpoints = append(mgmtEndpoints, "https://"+host)
		}
	}

	m.topologyCfgWatcher.Reconfigure(&ConfigWatcherHttpConfig{
		HttpRoundTripper: m.state.httpTransport,
		Endpoints:        mgmtEndpoints,
		UserAgent:        m.userAgent,
		Authenticator:    m.state.authenticator,
	})

	m.bucketsWatcher.Reconfigure(&StreamWatcherHttpConfig{
		HttpRoundTripper: m.state.httpTransport,
		Endpoints:        mgmtEndpoints,
		UserAgent:        m.userAgent,
		Authenticator:    m.state.authenticator,
	})
}

func (m *BucketsTrackingAgentManager) handleBuckets(buckets []bucketDescriptor) {
	m.bucketsLock.Lock()
	defer m.bucketsLock.Unlock()
	for _, bucket := range buckets {
		if _, ok := m.bucketAgents[bucket.Name]; !ok {
			m.opts.Logger.Debug("New bucket on cluster, creating agent", zap.String("name", bucket.Name))
			agent, err := m.makeAgentLocked(context.Background(), bucket.Name)
			if err != nil {
				m.opts.Logger.Debug("Failed to create agent", zap.String("name", bucket.Name))
				continue
			}

			m.bucketAgents[bucket.Name] = agent
		}
	}

	for bucket, agent := range m.bucketAgents {
		if !slices.ContainsFunc(buckets, func(descriptor bucketDescriptor) bool {
			return descriptor.Name == bucket
		}) {
			m.opts.Logger.Debug("Bucket no longer on cluster, shutting down agent", zap.String("name", bucket))
			delete(m.bucketAgents, bucket)
			agent.Close()
		}
	}

	if m.needsBucketChan != nil {
		close(m.needsBucketChan)
		m.needsBucketChan = nil
	}
}

func (m *BucketsTrackingAgentManager) makeAgentLocked(ctx context.Context, bucketName string) (*Agent, error) {
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

func (m *BucketsTrackingAgentManager) Reconfigure(opts BucketsTrackingAgentManagerReconfigureOptions) error {
	return errors.New("not yet supported")
}

func (m *BucketsTrackingAgentManager) GetClusterAgent(ctx context.Context) (*Agent, error) {
	if m.closed {
		return nil, errors.New("agent manager closed")
	}

	return m.clusterAgent, nil
}

func (m *BucketsTrackingAgentManager) GetBucketAgent(ctx context.Context, bucketName string) (*Agent, error) {
	m.bucketsLock.Lock()

	if m.closed {
		m.bucketsLock.Unlock()
		return nil, errors.New("agent manager closed")
	}
	m.bucketsLock.Unlock()

	for {
		m.bucketsLock.Lock()
		agent, ok := m.bucketAgents[bucketName]
		if !ok {
			if m.needsBucketChan == nil {
				m.needsBucketChan = make(chan struct{})
			}
			m.bucketsLock.Unlock()

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-m.needsBucketChan:
				continue
			}
		}
		m.bucketsLock.Unlock()

		return agent, nil
	}
}

// Close closes the AgentManager and all underlying Agent instances.
func (m *BucketsTrackingAgentManager) Close() error {
	m.bucketsLock.Lock()
	defer m.bucketsLock.Unlock()

	firstErr := m.clusterAgent.Close()
	for _, agent := range m.bucketAgents {
		err := agent.Close()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	m.watchersCancel()

	m.state.httpTransport.CloseIdleConnections()

	m.clusterAgent = nil
	m.bucketAgents = nil
	m.closed = true

	return firstErr
}
