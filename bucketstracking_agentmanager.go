package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
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
	IoConfig           IoConfig
	CreateAgentTimeout time.Duration
}

type bucketsTrackingAgentManagerState struct {
	tlsConfig     *tls.Config
	authenticator Authenticator
	httpTransport *http.Transport

	latestConfig atomic.Pointer[ParsedConfig]
}

type BucketsTrackingAgentManager struct {
	stateLock     sync.Mutex
	logger        *zap.Logger
	userAgent     string
	networkType   string
	localNodeAddr string

	compressionConfig  CompressionConfig
	configPollerConfig ConfigPollerConfig
	httpConfig         HTTPConfig
	ioConfig           IoConfig
	createAgentTimeout time.Duration

	state *bucketsTrackingAgentManagerState

	clusterAgent *Agent

	bucketsWatcher *BucketsWatcherHttp
	watchersCancel func()

	topologyCfgWatcher *TopologyWatcherHttp
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
		logger:        logger,
		userAgent:     httpUserAgent,
		networkType:   networkType,
		localNodeAddr: opts.SeedConfig.LocalNodeAddr,

		compressionConfig:  opts.CompressionConfig,
		configPollerConfig: opts.ConfigPollerConfig,
		httpConfig:         opts.HTTPConfig,
		ioConfig:           opts.IoConfig,
		createAgentTimeout: createAgentTimeout,

		state: &bucketsTrackingAgentManagerState{
			tlsConfig:     opts.TLSConfig,
			authenticator: opts.Authenticator,
			httpTransport: httpTransport,
		},
	}
	m.state.latestConfig.Store(bootstrapConfig)

	mgmtEndpoints := m.mgmtEndpointsLocked(bootstrapConfig)

	bucketsWatcher, err := NewBucketsWatcherHttp(BucketsWatcherHttpConfig{
		HttpRoundTripper: httpTransport,
		Endpoints:        mgmtEndpoints,
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

	configWatcher, err := NewConfigWatcherHttp(&ConfigWatcherHttpConfig{
		HttpRoundTripper: httpTransport,
		Endpoints:        mgmtEndpoints,
		UserAgent:        httpUserAgent,
		Authenticator:    opts.Authenticator,
	}, &ConfigWatcherHttpOptions{
		Logger: opts.Logger,
	})
	if err != nil {
		return nil, err
	}

	topoWatcher, err := NewTopologyWatcherHttp(
		configWatcher,
		&TopologyWatcherHttpOptions{
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

func (m *BucketsTrackingAgentManager) mgmtEndpointsLocked(cfg *ParsedConfig) []string {
	netInfo := cfg.AddressesGroupForNetworkType(m.networkType)

	var mgmtEndpoints []string
	tlsConfig := m.state.tlsConfig

	for _, node := range netInfo.Nodes {
		if tlsConfig == nil {
			mgmtEndpoints = append(mgmtEndpoints, fmt.Sprintf("http://%s:%d", node.Hostname, node.NonSSLPorts.Mgmt))
		} else {
			mgmtEndpoints = append(mgmtEndpoints, fmt.Sprintf("https://%s:%d", node.Hostname, node.SSLPorts.Mgmt))
		}
	}

	return mgmtEndpoints
}

func (m *BucketsTrackingAgentManager) applyConfig(config *ParsedConfig) {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()

	m.logger.Debug("Applying new config",
		zap.Int64("revId", config.RevID),
		zap.Int64("revEpoch", config.RevEpoch),
		zap.Any("config", config))

	m.state.latestConfig.Store(config)

	mgmtEndpoints := m.mgmtEndpointsLocked(config)

	err := m.topologyCfgWatcher.ConfigWatcher.Reconfigure(&ConfigWatcherHttpConfig{
		HttpRoundTripper: m.state.httpTransport,
		Endpoints:        mgmtEndpoints,
		UserAgent:        m.userAgent,
		Authenticator:    m.state.authenticator,
	})
	if err != nil {
		m.logger.Error("failed to reconfigure topology watcher", zap.Error(err))
	}

	err = m.bucketsWatcher.Reconfigure(&BucketsWatcherHttpReconfigureConfig{
		HttpRoundTripper: m.state.httpTransport,
		Endpoints:        mgmtEndpoints,
		Authenticator:    m.state.authenticator,
	})
	if err != nil {
		m.logger.Error("failed to reconfigure bucket watcher", zap.Error(err))
	}
}

func (m *BucketsTrackingAgentManager) makeAgent(ctx context.Context, bucketName string) (*Agent, error) {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()

	cfg := m.state.latestConfig.Load()
	netInfo := cfg.AddressesGroupForNetworkType(m.networkType)

	var kvDataAddrs []string
	var mgmtAddrs []string

	tlsConfig := m.state.tlsConfig
	for _, node := range netInfo.Nodes {
		if tlsConfig == nil {
			if node.HasData {
				kvDataAddrs = append(kvDataAddrs, fmt.Sprintf("%s:%d", node.Hostname, node.NonSSLPorts.Kv))
			}

			mgmtAddrs = append(mgmtAddrs, fmt.Sprintf("%s:%d", node.Hostname, node.NonSSLPorts.Mgmt))
		} else {
			if node.HasData {
				kvDataAddrs = append(kvDataAddrs, fmt.Sprintf("%s:%d", node.Hostname, node.SSLPorts.Kv))
			}

			mgmtAddrs = append(mgmtAddrs, fmt.Sprintf("%s:%d", node.Hostname, node.SSLPorts.Mgmt))
		}
	}

	ctx, cancel := context.WithTimeout(ctx, m.createAgentTimeout)
	defer cancel()

	// annotate the logger to indicate this is an agent under the
	// bucket tracking agentmanager.
	logger := m.logger.Named("agent")

	return CreateAgent(ctx, AgentOptions{
		Logger:        logger,
		TLSConfig:     m.state.tlsConfig,
		Authenticator: m.state.authenticator,
		SeedConfig: SeedConfig{
			HTTPAddrs:     mgmtAddrs,
			MemdAddrs:     kvDataAddrs,
			LocalNodeAddr: m.localNodeAddr,
		},
		CompressionConfig:  m.compressionConfig,
		ConfigPollerConfig: m.configPollerConfig,
		HTTPConfig:         m.httpConfig,
		IoConfig:           m.ioConfig,
		BucketName:         bucketName,
	})
}

func (m *BucketsTrackingAgentManager) WatchConfig(ctx context.Context) <-chan *ParsedConfig {
	return m.topologyCfgWatcher.Watch(ctx)
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
