package gocbcorex

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"go.uber.org/zap"
)

type agentState struct {
	bucket             string
	tlsConfig          *tls.Config
	authenticator      Authenticator
	numPoolConnections uint

	lastClients  map[string]*KvClientConfig
	latestConfig *ParsedConfig
}

type Agent struct {
	logger      *zap.Logger
	networkType string

	lock  sync.Mutex
	state agentState

	httpCfgWatcher *ConfigWatcherHttp
	memdCfgWatcher *ConfigWatcherMemd
	connMgr        KvClientManager
	collections    CollectionResolver
	retries        RetryManager
	vbRouter       VbucketRouter

	crud  *CrudComponent
	query *QueryComponent
	mgmt  *MgmtComponent
}

func CreateAgent(ctx context.Context, opts AgentOptions) (*Agent, error) {
	var srcHTTPAddrs []string
	for _, hostPort := range opts.SeedConfig.HTTPAddrs {
		if opts.TLSConfig == nil {
			ep := fmt.Sprintf("http://%s", hostPort)
			srcHTTPAddrs = append(srcHTTPAddrs, ep)
		} else {
			ep := fmt.Sprintf("https://%s", hostPort)
			srcHTTPAddrs = append(srcHTTPAddrs, ep)
		}
	}

	// Default values.
	compressionMinSize := 32
	compressionMinRatio := 0.83
	// httpIdleConnTimeout := 4500 * time.Millisecond
	// httpConnectTimeout := 30 * time.Second
	/*
		confHTTPRetryDelay := 10 * time.Second
		confHTTPRedialPeriod := 10 * time.Second
		confHTTPMaxWait := 5 * time.Second
	*/

	disableDecompression := opts.CompressionConfig.DisableDecompression
	useCompression := opts.CompressionConfig.EnableCompression

	if opts.CompressionConfig.MinSize > 0 {
		compressionMinSize = opts.CompressionConfig.MinSize
	}
	if opts.CompressionConfig.MinRatio > 0 {
		compressionMinRatio = opts.CompressionConfig.MinRatio
		if compressionMinRatio >= 1.0 {
			compressionMinRatio = 1.0
		}
	}
	/*
		if opts.HTTPConfig.IdleConnectionTimeout > 0 {
			httpIdleConnTimeout = opts.HTTPConfig.IdleConnectionTimeout
		}
		if opts.HTTPConfig.ConnectTimeout > 0 {
			httpConnectTimeout = opts.HTTPConfig.ConnectTimeout
		}
			if opts.ConfigPollerConfig.HTTPRetryDelay > 0 {
				confHTTPRetryDelay = opts.ConfigPollerConfig.HTTPRetryDelay
			}
			if opts.ConfigPollerConfig.HTTPRedialPeriod > 0 {
				confHTTPRedialPeriod = opts.ConfigPollerConfig.HTTPRedialPeriod
			}
			if opts.ConfigPollerConfig.HTTPMaxWait > 0 {
				confHTTPMaxWait = opts.ConfigPollerConfig.HTTPMaxWait
			}
	*/

	logger := loggerOrNop(opts.Logger)
	httpUserAgent := "gocbcorex/0.0.1-dev"

	httpDialer := &net.Dialer{
		// Timeout:   connectTimeout,
		KeepAlive: 30 * time.Second,
	}

	httpTransport := &http.Transport{
		ForceAttemptHTTP2: true,

		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return httpDialer.DialContext(ctx, network, addr)
		},
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			tcpConn, err := httpDialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}

			tlsConn := tls.Client(tcpConn, opts.TLSConfig)
			return tlsConn, nil
		},
		// MaxIdleConns:        maxIdleConns,
		// MaxIdleConnsPerHost: maxIdleConnsPerHost,
		// IdleConnTimeout:     idleTimeout,
	}

	bootstrapper, err := NewConfigBootstrapHttp(ConfigBoostrapHttpOptions{
		Logger:           logger.Named("http-bootstrap"),
		HttpRoundTripper: httpTransport,
		Endpoints:        srcHTTPAddrs,
		UserAgent:        httpUserAgent,
		Authenticator:    opts.Authenticator,
		BucketName:       opts.BucketName,
	})
	if err != nil {
		return nil, err
	}

	bootstrapConfig, networkType, err := bootstrapper.Bootstrap(ctx)
	if err != nil {
		return nil, err
	}

	logger.Debug("agent bootstrapped",
		zap.Any("bootstrapConfig", bootstrapConfig),
		zap.String("networkType", networkType))

	agent := &Agent{
		logger:      logger,
		networkType: networkType,

		state: agentState{
			bucket:             opts.BucketName,
			tlsConfig:          opts.TLSConfig,
			authenticator:      opts.Authenticator,
			numPoolConnections: 1,
			latestConfig:       bootstrapConfig,
		},

		retries: NewRetryManagerFastFail(),
	}

	agentComponentConfigs := agent.genAgentComponentConfigsLocked()

	connMgr, err := NewKvClientManager(&KvClientManagerConfig{
		NumPoolConnections: agent.state.numPoolConnections,
		Clients:            agentComponentConfigs.KvClientManagerClients,
	}, &KvClientManagerOptions{
		Logger: agent.logger,
	})
	if err != nil {
		return nil, err
	}
	agent.connMgr = connMgr

	coreCollections, err := NewCollectionResolverMemd(&CollectionResolverMemdOptions{
		Logger:  agent.logger,
		ConnMgr: agent.connMgr,
	})
	if err != nil {
		return nil, err
	}
	collections, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Logger:         agent.logger,
		Resolver:       coreCollections,
		ResolveTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	agent.collections = collections

	agent.vbRouter = NewVbucketRouter(&VbucketRouterOptions{
		Logger: agent.logger,
	})
	agent.vbRouter.UpdateRoutingInfo(agentComponentConfigs.VbucketRoutingInfo)

	if false {
		configWatcher, err := NewConfigWatcherHttp(
			&agentComponentConfigs.ConfigWatcherHttpConfig,
			&ConfigWatcherHttpOptions{
				Logger: logger.Named("http-config-watcher"),
			})
		if err != nil {
			return nil, err
		}

		agent.httpCfgWatcher = configWatcher
	} else {
		configWatcher, err := NewConfigWatcherMemd(
			&agentComponentConfigs.ConfigWatcherMemdConfig,
			&ConfigWatcherMemdOptions{
				Logger:          logger.Named("memd-config-watcher"),
				KvClientManager: connMgr,
				PollingPeriod:   2500 * time.Millisecond,
			},
		)
		if err != nil {
			return nil, err
		}

		agent.memdCfgWatcher = configWatcher
	}

	go agent.configWatcherThread()

	agent.crud = &CrudComponent{
		logger:      agent.logger,
		collections: agent.collections,
		retries:     agent.retries,
		connManager: agent.connMgr,
		nmvHandler:  &agentNmvHandler{agent},
		vbs:         agent.vbRouter,
		compression: &CompressionManagerDefault{
			disableCompression:   !useCompression,
			compressionMinSize:   compressionMinSize,
			compressionMinRatio:  compressionMinRatio,
			disableDecompression: disableDecompression,
		},
	}
	agent.query = NewQueryComponent(
		agent.retries,
		&agentComponentConfigs.QueryComponentConfig,
		&QueryComponentOptions{
			Logger:    logger,
			UserAgent: httpUserAgent,
		},
	)
	agent.mgmt = NewMgmtComponent(
		agent.retries,
		&agentComponentConfigs.MgmtComponentConfig,
		&MgmtComponentOptions{
			Logger:    logger,
			UserAgent: httpUserAgent,
		},
	)

	return agent, nil
}

type agentComponentConfigs struct {
	ConfigWatcherHttpConfig ConfigWatcherHttpConfig
	ConfigWatcherMemdConfig ConfigWatcherMemdConfig
	KvClientManagerClients  map[string]*KvClientConfig
	VbucketRoutingInfo      *VbucketRoutingInfo
	QueryComponentConfig    QueryComponentConfig
	MgmtComponentConfig     MgmtComponentConfig
}

func (agent *Agent) genAgentComponentConfigsLocked() *agentComponentConfigs {
	httpUserAgent := "gocbcorex/0.0.1-dev"

	bootstrapHosts := agent.state.latestConfig.AddressesGroupForNetworkType(agent.networkType)

	var kvDataNodeIds []string
	var kvDataHosts []string
	var mgmtEndpoints []string
	var queryEndpoints []string
	var searchEndpoints []string
	if agent.state.tlsConfig == nil {
		kvDataNodeIds = bootstrapHosts.NonSSL.KvData
		kvDataHosts = bootstrapHosts.NonSSL.KvData
		for _, host := range bootstrapHosts.NonSSL.Mgmt {
			mgmtEndpoints = append(mgmtEndpoints, "http://"+host)
		}
		for _, host := range bootstrapHosts.NonSSL.Query {
			queryEndpoints = append(queryEndpoints, "http://"+host)
		}
		for _, host := range bootstrapHosts.NonSSL.Search {
			searchEndpoints = append(searchEndpoints, "http://"+host)
		}
	} else {
		kvDataNodeIds = bootstrapHosts.SSL.KvData
		kvDataHosts = bootstrapHosts.SSL.KvData
		for _, host := range bootstrapHosts.SSL.Mgmt {
			mgmtEndpoints = append(mgmtEndpoints, "https://"+host)
		}
		for _, host := range bootstrapHosts.SSL.Query {
			queryEndpoints = append(queryEndpoints, "https://"+host)
		}
		for _, host := range bootstrapHosts.SSL.Search {
			searchEndpoints = append(searchEndpoints, "https://"+host)
		}
	}

	clients := make(map[string]*KvClientConfig)
	for addrIdx, addr := range kvDataHosts {
		nodeId := kvDataNodeIds[addrIdx]
		clients[nodeId] = &KvClientConfig{
			Logger:         agent.logger,
			Address:        addr,
			TlsConfig:      agent.state.tlsConfig,
			SelectedBucket: agent.state.bucket,
			Authenticator:  agent.state.authenticator,
		}
	}

	httpDialer := &net.Dialer{
		// Timeout:   connectTimeout,
		KeepAlive: 30 * time.Second,
	}

	httpTransport := &http.Transport{
		ForceAttemptHTTP2: true,

		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return httpDialer.DialContext(ctx, network, addr)
		},
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			tcpConn, err := httpDialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}

			tlsConn := tls.Client(tcpConn, agent.state.tlsConfig)
			return tlsConn, nil
		},
		// MaxIdleConns:        maxIdleConns,
		// MaxIdleConnsPerHost: maxIdleConnsPerHost,
		// IdleConnTimeout:     idleTimeout,
	}

	return &agentComponentConfigs{
		ConfigWatcherHttpConfig: ConfigWatcherHttpConfig{
			HttpRoundTripper: httpTransport,
			Endpoints:        mgmtEndpoints,
			UserAgent:        httpUserAgent,
			Authenticator:    agent.state.authenticator,
			BucketName:       agent.state.bucket,
		},
		ConfigWatcherMemdConfig: ConfigWatcherMemdConfig{
			Endpoints: kvDataNodeIds,
		},
		KvClientManagerClients: clients,
		VbucketRoutingInfo: &VbucketRoutingInfo{
			VbMap:      agent.state.latestConfig.VbucketMap,
			ServerList: kvDataNodeIds,
		},
		QueryComponentConfig: QueryComponentConfig{
			HttpRoundTripper: httpTransport,
			Endpoints:        queryEndpoints,
			Authenticator:    agent.state.authenticator,
		},
		MgmtComponentConfig: MgmtComponentConfig{
			HttpRoundTripper: httpTransport,
			Endpoints:        mgmtEndpoints,
			Authenticator:    agent.state.authenticator,
		},
	}
}

func (agent *Agent) Reconfigure(opts *AgentReconfigureOptions) error {
	agent.lock.Lock()
	defer agent.lock.Unlock()

	agent.state.tlsConfig = opts.TLSConfig
	agent.state.authenticator = opts.Authenticator
	agent.updateStateLocked()

	return nil
}

func (agent *Agent) Close() error {
	return nil
}

func (agent *Agent) applyConfig(config *ParsedConfig) {
	agent.lock.Lock()
	defer agent.lock.Unlock()

	// Check that the new config data is newer than the current one, in the case where we've done a select bucket
	// against an existing connection then the revisions could be the same. In that case the configuration still
	// needs to be applied.
	// In the case where the rev epochs are the same then we need to compare rev IDs. If the new config epoch is lower
	// than the old one then we ignore it, if it's newer then we apply the new config.
	oldConfig := agent.state.latestConfig
	if config.BucketType != oldConfig.BucketType {
		agent.logger.Debug("switching config due to changed bucket type")
	} else if !oldConfig.IsVersioned() {
		agent.logger.Debug("switching config due to unversioned old config")
	} else {
		delta := oldConfig.Compare(config)
		if delta > 0 {
			agent.logger.Debug("skipping config due to new config being an older revision")
			return
		} else if delta == 0 {
			agent.logger.Debug("skipping config due to matching revisions")
			return
		}
	}

	agent.state.latestConfig = config
	agent.updateStateLocked()
}

func (agent *Agent) updateStateLocked() {
	agent.logger.Debug("updating components",
		zap.Any("state", agent.state),
		zap.Any("config", *agent.state.latestConfig))

	agentComponentConfigs := agent.genAgentComponentConfigsLocked()

	// In order to avoid race conditions between operations selecting the
	// endpoint they need to send the request to, and fetching an actual
	// client which can send to that endpoint.  We must first ensure that
	// all the new endpoints are available in the manager.  Then update
	// the routing table.  Then go back and remove the old entries from
	// the connection manager list.

	oldClients := make(map[string]*KvClientConfig)
	if agent.state.lastClients != nil {
		for clientName, client := range agent.state.lastClients {
			oldClients[clientName] = client
		}
	}
	for clientName, client := range agentComponentConfigs.KvClientManagerClients {
		if oldClients[clientName] == nil {
			oldClients[clientName] = client
		}
	}

	agent.connMgr.Reconfigure(&KvClientManagerConfig{
		NumPoolConnections: agent.state.numPoolConnections,
		Clients:            oldClients,
	})

	agent.vbRouter.UpdateRoutingInfo(agentComponentConfigs.VbucketRoutingInfo)

	if agent.memdCfgWatcher != nil {
		agent.memdCfgWatcher.Reconfigure(&agentComponentConfigs.ConfigWatcherMemdConfig)
	}

	agent.connMgr.Reconfigure(&KvClientManagerConfig{
		NumPoolConnections: agent.state.numPoolConnections,
		Clients:            agentComponentConfigs.KvClientManagerClients,
	})

	agent.query.Reconfigure(&agentComponentConfigs.QueryComponentConfig)
	agent.mgmt.Reconfigure(&agentComponentConfigs.MgmtComponentConfig)

	if agent.httpCfgWatcher != nil {
		agent.httpCfgWatcher.Reconfigure(&agentComponentConfigs.ConfigWatcherHttpConfig)
	}
}

func (agent *Agent) configWatcherThread() {
	var configCh <-chan *ParsedConfig
	if agent.memdCfgWatcher != nil {
		configCh = agent.memdCfgWatcher.Watch(context.Background())
	} else if agent.httpCfgWatcher != nil {
		configCh = agent.httpCfgWatcher.Watch(context.Background())
	} else {
		agent.logger.Warn("failed to start config monitoring due to missing watcher")
		return
	}

	for config := range configCh {
		agent.applyConfig(config)
	}
}

func (a *Agent) applyTerseConfigJson(config *cbconfig.TerseConfigJson, sourceHostname string) {
	parsedConfig, err := ConfigParser{}.ParseTerseConfig(config, sourceHostname)
	if err != nil {
		a.logger.Warn("failed to process a not-my-vbucket configuration", zap.Error(err))
		return
	}

	a.applyConfig(parsedConfig)
}

func (a *Agent) handleNotMyVbucketConfig(config *cbconfig.TerseConfigJson, sourceHostname string) {
	a.applyTerseConfigJson(config, sourceHostname)
}

// agentConfigHandler exists for the purpose of satisfying the NotMyVbucketConfigHandler interface
// for Agent, without having to publicly expose the function on Agent itself.
type agentNmvHandler struct {
	agent *Agent
}

func (h *agentNmvHandler) HandleNotMyVbucketConfig(config *cbconfig.TerseConfigJson, sourceHostname string) {
	h.agent.handleNotMyVbucketConfig(config, sourceHostname)
}
