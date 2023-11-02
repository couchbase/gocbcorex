package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"sync"
	"time"

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
	CreateAgentTimeout time.Duration
}

type bucketsTrackingAgentManagerState struct {
	tlsConfig     *tls.Config
	authenticator Authenticator
	httpTransport *http.Transport

	latestConfig AtomicPointer[ParsedConfig]
}

type BucketsTrackingAgentManager struct {
	stateLock   sync.Mutex
	logger      *zap.Logger
	userAgent   string
	networkType string

	compressionConfig  CompressionConfig
	configPollerConfig ConfigPollerConfig
	httpConfig         HTTPConfig
	createAgentTimeout time.Duration

	state *bucketsTrackingAgentManagerState

	clusterAgent *Agent

	bucketsWatcher *BucketsWatcherHttp
	watchersCancel func()

	topologyCfgWatcher *ConfigWatcherHttp

	newCfgSig     chan struct{}
	newCfgSigLock sync.Mutex
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

	createAgentTimeout := opts.CreateAgentTimeout
	if createAgentTimeout == 0 {
		createAgentTimeout = 7 * time.Second
	}

	m := &BucketsTrackingAgentManager{
		logger:      logger,
		userAgent:   httpUserAgent,
		networkType: networkType,

		compressionConfig:  opts.CompressionConfig,
		configPollerConfig: opts.ConfigPollerConfig,
		httpConfig:         opts.HTTPConfig,
		createAgentTimeout: createAgentTimeout,

		newCfgSig: make(chan struct{}),

		state: &bucketsTrackingAgentManagerState{
			tlsConfig:     opts.TLSConfig,
			authenticator: opts.Authenticator,
			httpTransport: httpTransport,
		},
	}
	m.state.latestConfig.Store(bootstrapConfig)

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

func (m *BucketsTrackingAgentManager) mgmtEndpoints(cfg *ParsedConfig) []string {
	bootstrapHosts := cfg.AddressesGroupForNetworkType(m.networkType)

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

	currentCfg := m.state.latestConfig.Load()
	if currentCfg == nil {
		// We're shutting down so do nothing.
		return
	}

	if !canUpdateConfig(config, currentCfg, m.logger) {
		return
	}

	m.logger.Debug("Applying new config", zap.Int64("revId", config.RevID), zap.Int64("revEpoch", config.RevEpoch))

	m.state.latestConfig.Store(config)

	mgmtEndpoints := m.mgmtEndpoints(config)

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

	m.newCfgSigLock.Lock()
	close(m.newCfgSig)
	m.newCfgSig = make(chan struct{})
	m.newCfgSigLock.Unlock()
}

func (m *BucketsTrackingAgentManager) makeAgent(ctx context.Context, bucketName string) (*Agent, error) {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()

	cfg := m.state.latestConfig.Load()
	bootstrapHosts := cfg.AddressesGroupForNetworkType(m.networkType)

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

	ctx, cancel := context.WithTimeout(ctx, m.createAgentTimeout)
	defer cancel()

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

func (m *BucketsTrackingAgentManager) Watch(ctx context.Context) <-chan *ParsedConfig {
	outCh := make(chan *ParsedConfig, 1)
	go func() {
		for {
			m.newCfgSigLock.Lock()
			newCfgSig := m.newCfgSig
			m.newCfgSigLock.Unlock()

			if newCfgSig == nil {
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-newCfgSig:
			}

			cfg := m.state.latestConfig.Load()
			if cfg == nil {
				return
			}

			outCh <- cfg
		}

	}()
	return outCh
}

func (m *BucketsTrackingAgentManager) Reconfigure(opts BucketsTrackingAgentManagerReconfigureOptions) error {
	return errors.New("not yet supported")
}

func (m *BucketsTrackingAgentManager) GetClusterAgent(ctx context.Context) (*Agent, error) {
	if m.state.latestConfig.Load() == nil {
		return nil, errors.New("agent manager closed")
	}

	return m.clusterAgent, nil
}

func (m *BucketsTrackingAgentManager) GetBucketAgent(ctx context.Context, bucketName string) (*Agent, error) {
	if m.state.latestConfig.Load() == nil {
		return nil, errors.New("agent manager closed")
	}

	return m.bucketsWatcher.GetAgent(ctx, bucketName)
}

// Close closes the AgentManager and all underlying Agent instances.
func (m *BucketsTrackingAgentManager) Close() error {
	m.logger.Debug("Closing")

	m.state.latestConfig.Store(nil)
	m.newCfgSigLock.Lock()
	// Make sure that any Watch requests that come in don't end up hanging.
	close(m.newCfgSig)
	m.newCfgSig = nil
	m.newCfgSigLock.Unlock()

	firstErr := m.clusterAgent.Close()

	err := m.bucketsWatcher.Close()
	if err != nil && firstErr == nil {
		firstErr = err
	}

	m.stateLock.Lock()
	m.clusterAgent = nil
	m.state.httpTransport.CloseIdleConnections()
	m.stateLock.Unlock()

	m.watchersCancel()

	return firstErr
}
