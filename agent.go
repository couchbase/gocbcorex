package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/contrib/buildversion"
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"go.uber.org/zap"
)

var buildVersion string = buildversion.GetVersion("github.com/couchbase/gocbcorex")

type agentState struct {
	bucket             string
	tlsConfig          *tls.Config
	authenticator      Authenticator
	numPoolConnections uint
	httpTransport      *http.Transport

	lastClients  map[string]*KvClientConfig
	latestConfig *ParsedConfig
}

type Agent struct {
	logger      *zap.Logger
	networkType string

	lock  sync.Mutex
	state agentState

	cfgWatcher  ConfigWatcher
	connMgr     KvClientManager
	collections CollectionResolver
	retries     RetryManager
	vbRouter    VbucketRouter

	httpCfgWatcher   *ConfigWatcherHttp
	memdCfgWatcher   *ConfigWatcherMemd
	cfgWatcherCancel func()

	crud   *CrudComponent
	query  *QueryComponent
	mgmt   *MgmtComponent
	search *SearchComponent
}

func CreateAgent(ctx context.Context, opts AgentOptions) (*Agent, error) {
	logger := loggerOrNop(opts.Logger)

	logger.Debug("Creating new agent",
		zap.Object("config", opts),
		zap.String("build-version", buildVersion))

	clientName := fmt.Sprintf("gocbcorex/%s", buildVersion)
	srcHTTPAddrs := makeSrcHTTPAddrs(opts.SeedConfig.HTTPAddrs, opts.TLSConfig)

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

	httpTransport := makeHTTPTransport(opts.TLSConfig)
	handleAgentCreateErr := func(err error) error {
		httpTransport.CloseIdleConnections()

		return err
	}

	bootstrapper, err := NewConfigBootstrapHttp(ConfigBoostrapHttpOptions{
		Logger:           logger.Named("http-bootstrap"),
		HttpRoundTripper: httpTransport,
		Endpoints:        srcHTTPAddrs,
		UserAgent:        clientName,
		Authenticator:    opts.Authenticator,
		BucketName:       opts.BucketName,
	})
	if err != nil {
		return nil, handleAgentCreateErr(err)
	}

	bootstrapConfig, networkType, err := bootstrapper.Bootstrap(ctx)
	if err != nil {
		return nil, handleAgentCreateErr(err)
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
			httpTransport:      httpTransport,
		},
	}
	if opts.RetryManager == nil {
		agent.retries = NewRetryManagerDefault()
	} else {
		agent.retries = opts.RetryManager
	}

	agentComponentConfigs := agent.genAgentComponentConfigsLocked()

	connMgr, err := NewKvClientManager(&KvClientManagerConfig{
		NumPoolConnections: agent.state.numPoolConnections,
		Clients:            agentComponentConfigs.KvClientManagerClients,
	}, &KvClientManagerOptions{
		Logger: agent.logger.Named("client-manager"),
	})
	if err != nil {
		return nil, handleAgentCreateErr(err)
	}
	agent.connMgr = connMgr

	coreCollections, err := NewCollectionResolverMemd(&CollectionResolverMemdOptions{
		Logger:  agent.logger,
		ConnMgr: agent.connMgr,
	})
	if err != nil {
		return nil, handleAgentCreateErr(err)
	}
	collections, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Logger:         agent.logger,
		Resolver:       coreCollections,
		ResolveTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, handleAgentCreateErr(err)
	}
	agent.collections = collections

	agent.vbRouter = NewVbucketRouter(&VbucketRouterOptions{
		Logger: agent.logger,
	})
	agent.vbRouter.UpdateRoutingInfo(agentComponentConfigs.VbucketRoutingInfo)

	if opts.BucketName == "" {
		configWatcher, err := NewConfigWatcherHttp(
			&agentComponentConfigs.ConfigWatcherHttpConfig,
			&ConfigWatcherHttpOptions{
				Logger: logger.Named("http-config-watcher"),
			})
		if err != nil {
			return nil, handleAgentCreateErr(err)
		}

		agent.httpCfgWatcher = configWatcher
		agent.cfgWatcher = configWatcher
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
			return nil, handleAgentCreateErr(err)
		}

		agent.memdCfgWatcher = configWatcher
		agent.cfgWatcher = configWatcher
	}

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
			UserAgent: clientName,
		},
	)
	agent.mgmt = NewMgmtComponent(
		agent.retries,
		&agentComponentConfigs.MgmtComponentConfig,
		&MgmtComponentOptions{
			Logger:    logger,
			UserAgent: clientName,
		},
	)
	agent.search = NewSearchComponent(
		agent.retries,
		&agentComponentConfigs.SearchComponentConfig,
		&SearchComponentOptions{
			Logger:    logger,
			UserAgent: clientName,
		},
	)

	agent.startConfigWatcher()

	return agent, nil
}

type agentComponentConfigs struct {
	ConfigWatcherHttpConfig ConfigWatcherHttpConfig
	ConfigWatcherMemdConfig ConfigWatcherMemdConfig
	KvClientManagerClients  map[string]*KvClientConfig
	VbucketRoutingInfo      *VbucketRoutingInfo
	QueryComponentConfig    QueryComponentConfig
	MgmtComponentConfig     MgmtComponentConfig
	SearchComponentConfig   SearchComponentConfig
}

func (agent *Agent) genAgentComponentConfigsLocked() *agentComponentConfigs {
	clientName := fmt.Sprintf("gocbcorex/%s", buildVersion)

	bootstrapHosts := agent.state.latestConfig.AddressesGroupForNetworkType(agent.networkType)

	var kvDataHosts []string
	var mgmtEndpoints []string
	var queryEndpoints []string
	var searchEndpoints []string

	tlsConfig := agent.state.tlsConfig
	if tlsConfig == nil {
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
	kvDataNodeIds := make([]string, len(bootstrapHosts.NonSSL.KvData))
	for i, hostPort := range bootstrapHosts.NonSSL.KvData {
		kvDataNodeIds[i] = "ep-" + strings.Replace(hostPort, ":", "-", -1)
	}

	clients := make(map[string]*KvClientConfig)
	for addrIdx, addr := range kvDataHosts {
		nodeId := kvDataNodeIds[addrIdx]
		clients[nodeId] = &KvClientConfig{
			Address:        addr,
			TlsConfig:      tlsConfig,
			ClientName:     clientName,
			SelectedBucket: agent.state.bucket,
			Authenticator:  agent.state.authenticator,
		}
	}

	return &agentComponentConfigs{
		ConfigWatcherHttpConfig: ConfigWatcherHttpConfig{
			HttpRoundTripper: agent.state.httpTransport,
			Endpoints:        mgmtEndpoints,
			UserAgent:        clientName,
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
			HttpRoundTripper: agent.state.httpTransport,
			Endpoints:        queryEndpoints,
			Authenticator:    agent.state.authenticator,
		},
		MgmtComponentConfig: MgmtComponentConfig{
			HttpRoundTripper: agent.state.httpTransport,
			Endpoints:        mgmtEndpoints,
			Authenticator:    agent.state.authenticator,
		},
		SearchComponentConfig: SearchComponentConfig{
			HttpRoundTripper: agent.state.httpTransport,
			Endpoints:        searchEndpoints,
			Authenticator:    agent.state.authenticator,
		},
	}
}

func (agent *Agent) Reconfigure(opts *AgentReconfigureOptions) error {
	agent.lock.Lock()
	defer agent.lock.Unlock()

	if agent.state.bucket != "" {
		if opts.BucketName != agent.state.bucket {
			return errors.New("cannot change an already-specified bucket name")
		}
	}

	agent.state.tlsConfig = opts.TLSConfig
	agent.state.authenticator = opts.Authenticator
	agent.state.bucket = opts.BucketName

	// Close the old http transport and make a new one with the new tls config.
	agent.state.httpTransport.CloseIdleConnections()

	agent.state.httpTransport = makeHTTPTransport(opts.TLSConfig)

	agent.updateStateLocked()

	return nil
}

func (agent *Agent) BucketName() string {
	agent.lock.Lock()
	defer agent.lock.Unlock()

	return agent.state.bucket
}

func (agent *Agent) Close() error {
	if err := agent.connMgr.Close(); err != nil {
		agent.logger.Debug("Failed to close conn mgr", zap.Error(err))
	}

	agent.cfgWatcherCancel()
	agent.state.httpTransport.CloseIdleConnections()
	return nil
}

func (agent *Agent) WatchConfig(ctx context.Context) <-chan *ParsedConfig {
	return agent.cfgWatcher.Watch(ctx)
}

func (agent *Agent) applyConfig(config *ParsedConfig) {
	agent.lock.Lock()
	defer agent.lock.Unlock()

	if !canUpdateConfig(config, agent.state.latestConfig, agent.logger) {
		return
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

	err := agent.connMgr.Reconfigure(&KvClientManagerConfig{
		NumPoolConnections: agent.state.numPoolConnections,
		Clients:            oldClients,
	}, func(error) {})
	if err != nil {
		agent.logger.Error("failed to reconfigure connection manager (old clients)", zap.Error(err))
	}

	agent.vbRouter.UpdateRoutingInfo(agentComponentConfigs.VbucketRoutingInfo)

	if agent.memdCfgWatcher != nil {
		err = agent.memdCfgWatcher.Reconfigure(&agentComponentConfigs.ConfigWatcherMemdConfig)
		if err != nil {
			agent.logger.Error("failed to reconfigure memd config watcher component", zap.Error(err))
		}
	}

	err = agent.connMgr.Reconfigure(&KvClientManagerConfig{
		NumPoolConnections: agent.state.numPoolConnections,
		Clients:            agentComponentConfigs.KvClientManagerClients,
	}, func(error) {})
	if err != nil {
		agent.logger.Error("failed to reconfigure connection manager (updated clients)", zap.Error(err))
	}

	err = agent.query.Reconfigure(&agentComponentConfigs.QueryComponentConfig)
	if err != nil {
		agent.logger.Error("failed to reconfigure query component", zap.Error(err))
	}

	err = agent.mgmt.Reconfigure(&agentComponentConfigs.MgmtComponentConfig)
	if err != nil {
		agent.logger.Error("failed to reconfigure management component", zap.Error(err))
	}

	err = agent.search.Reconfigure(&agentComponentConfigs.SearchComponentConfig)
	if err != nil {
		agent.logger.Error("failed to reconfigure query component", zap.Error(err))
	}

	if agent.httpCfgWatcher != nil {
		err = agent.httpCfgWatcher.Reconfigure(&agentComponentConfigs.ConfigWatcherHttpConfig)
		if err != nil {
			agent.logger.Error("failed to reconfigure http config watcher component", zap.Error(err))
		}
	}
}

func (agent *Agent) startConfigWatcher() {
	ctx, cancel := context.WithCancel(context.Background())

	configCh := agent.cfgWatcher.Watch(ctx)
	stoppedSig := make(chan struct{})
	agent.cfgWatcherCancel = func() {
		// Cancel the context and wait for the configCh to be closed.
		cancel()
		<-stoppedSig
	}

	// We only watch for new configs, rather than also initiating Watch, in its own goroutine so that agent startup
	// and close can't race on the cfgWatcherCancel.
	go func() {
		for config := range configCh {
			agent.applyConfig(config)
		}
		close(stoppedSig)
	}()
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

func makeHTTPTransport(tlsConfig *tls.Config) *http.Transport {
	httpDialer := &net.Dialer{
		// Timeout:   connectTimeout,
		KeepAlive: 30 * time.Second,
	}

	return &http.Transport{
		ForceAttemptHTTP2: true,

		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return httpDialer.DialContext(ctx, network, addr)
		},
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			tcpConn, err := httpDialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}

			tlsConn := tls.Client(tcpConn, tlsConfig)
			return tlsConn, nil
		},
		// MaxIdleConns:        maxIdleConns,
		// MaxIdleConnsPerHost: maxIdleConnsPerHost,
		// IdleConnTimeout:     idleTimeout,
	}
}

func makeSrcHTTPAddrs(seedAddrs []string, tlsConfig *tls.Config) []string {
	var srcHTTPAddrs []string
	for _, hostPort := range seedAddrs {
		if tlsConfig == nil {
			ep := fmt.Sprintf("http://%s", hostPort)
			srcHTTPAddrs = append(srcHTTPAddrs, ep)
		} else {
			ep := fmt.Sprintf("https://%s", hostPort)
			srcHTTPAddrs = append(srcHTTPAddrs, ep)
		}
	}

	return srcHTTPAddrs
}

func canUpdateConfig(newConfig, oldConfig *ParsedConfig, logger *zap.Logger) bool {
	// Check that the new config data is newer than the current one, in the case where we've done a select bucket
	// against an existing connection then the revisions could be the same. In that case the configuration still
	// needs to be applied.
	// In the case where the rev epochs are the same then we need to compare rev IDs. If the new config epoch is lower
	// than the old one then we ignore it, if it's newer then we apply the new config.
	if newConfig.BucketType != oldConfig.BucketType {
		logger.Debug("switching config due to changed bucket type")
	} else if !oldConfig.IsVersioned() {
		logger.Debug("switching config due to unversioned old config")
	} else {
		delta := oldConfig.Compare(newConfig)
		if delta > 0 {
			logger.Debug("skipping config due to new config being an older revision")
			return false
		} else if delta == 0 {
			logger.Debug("skipping config due to matching revisions")
			return false
		}
	}

	return true
}
