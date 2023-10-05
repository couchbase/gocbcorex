package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"sync"

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
	stateLock   sync.Mutex
	logger      *zap.Logger
	userAgent   string
	networkType string

	compressionConfig  CompressionConfig
	configPollerConfig ConfigPollerConfig
	httpConfig         HTTPConfig

	state *bucketsTrackingAgentManagerState

	clusterAgent *Agent
	closed       bool

	bucketsWatcher *BucketsWatcherHttp
	watchersCancel func()

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
	logger = logger.Named("tracking-agent-manager")

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
		logger:      logger,
		userAgent:   httpUserAgent,
		networkType: networkType,

		compressionConfig:  opts.CompressionConfig,
		configPollerConfig: opts.ConfigPollerConfig,
		httpConfig:         opts.HTTPConfig,

		state: &bucketsTrackingAgentManagerState{
			tlsConfig:     opts.TLSConfig,
			authenticator: opts.Authenticator,
			httpTransport: httpTransport,
			latestConfig:  bootstrapConfig,
		},
		clusterAgent:       nil,
		closed:             false,
		bucketsWatcher:     nil,
		watchersCancel:     nil,
		topologyCfgWatcher: nil,
	}

	bucketsWatcher, err := NewBucketsWatcherHttp(BucketsWatcherHttpConfig{
		HttpRoundTripper: httpTransport,
		Endpoints:        srcHTTPAddrs,
		UserAgent:        httpUserAgent,
		Authenticator:    opts.Authenticator,
		MakeAgent:        m.makeAgent,
	}, BucketsWatcherHttpOptions{
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

	clusterAgent, err := m.makeAgent(ctx, "")
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
	m.watchersCancel = func() {
		// Cancel the context and wait for the configCh to be closed.
		cancel()
		m.logger.Debug("Waiting for topology watcher shutdown")
		<-stoppedTopoSig
		m.logger.Debug("Completed topology watcher shutdown")
	}

	m.bucketsWatcher.Watch()

	topoCh := m.topologyCfgWatcher.Watch(ctx)

	go func() {
		for config := range topoCh {
			m.applyConfig(config)
		}
		close(stoppedTopoSig)
	}()
}

func (m *BucketsTrackingAgentManager) mgmtEndpointsLocked() []string {
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

	return mgmtEndpoints
}

func (m *BucketsTrackingAgentManager) applyConfig(config *ParsedConfig) {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()

	if !canUpdateConfig(config, m.state.latestConfig, m.logger) {
		return
	}

	m.logger.Debug("Applying new config", zap.Int64("revId", config.RevID), zap.Int64("revEpoch", config.RevEpoch))

	m.state.latestConfig = config

	mgmtEndpoints := m.mgmtEndpointsLocked()

	m.topologyCfgWatcher.Reconfigure(&ConfigWatcherHttpConfig{
		HttpRoundTripper: m.state.httpTransport,
		Endpoints:        mgmtEndpoints,
		UserAgent:        m.userAgent,
		Authenticator:    m.state.authenticator,
	})

	m.bucketsWatcher.Reconfigure(&BucketsWatcherHttpReconfigureConfig{
		HttpRoundTripper: m.state.httpTransport,
		Endpoints:        mgmtEndpoints,
		Authenticator:    m.state.authenticator,
	})
}

func (m *BucketsTrackingAgentManager) makeAgent(ctx context.Context, bucketName string) (*Agent, error) {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()

	bootstrapHosts := m.state.latestConfig.AddressesGroupForNetworkType(m.networkType)

	var kvDataHosts []string
	var mgmtEndpoints []string

	tlsConfig := m.state.tlsConfig
	if tlsConfig == nil {
		kvDataHosts = bootstrapHosts.NonSSL.KvData
		mgmtEndpoints = bootstrapHosts.NonSSL.Mgmt
	} else {
		kvDataHosts = bootstrapHosts.SSL.KvData
		mgmtEndpoints = bootstrapHosts.SSL.Mgmt
	}

	return CreateAgent(ctx, AgentOptions{
		Logger:        m.logger,
		TLSConfig:     m.state.tlsConfig,
		Authenticator: m.state.authenticator,
		SeedConfig: SeedConfig{
			HTTPAddrs: mgmtEndpoints,
			MemdAddrs: kvDataHosts,
		},
		CompressionConfig:  m.compressionConfig,
		ConfigPollerConfig: m.configPollerConfig,
		HTTPConfig:         m.httpConfig,
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
	m.stateLock.Lock()
	if m.closed {
		m.stateLock.Unlock()
		return nil, errors.New("agent manager closed")
	}
	m.stateLock.Unlock()

	return m.bucketsWatcher.GetAgent(ctx, bucketName)
}

// Close closes the AgentManager and all underlying Agent instances.
func (m *BucketsTrackingAgentManager) Close() error {
	m.logger.Debug("Closing")

	firstErr := m.clusterAgent.Close()

	err := m.bucketsWatcher.Close()
	if err != nil && firstErr == nil {
		firstErr = err
	}

	m.stateLock.Lock()
	m.clusterAgent = nil
	m.closed = true
	m.state.httpTransport.CloseIdleConnections()
	m.stateLock.Unlock()

	m.watchersCancel()

	return firstErr
}
