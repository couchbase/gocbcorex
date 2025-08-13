package gocbcorex

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type DcpStreamOptions struct {
	ConnectionName        string
	ConsumerName          string
	NoopInterval          time.Duration
	Priority              string
	ForceValueCompression bool
	EnableExpiryEvents    bool
	EnableStreamIds       bool
	EnableOso             bool
	EnableSeqNoAdvance    bool
	BackfillOrder         string
	EnableChangeStreams   bool
}

type DcpAgentOptions struct {
	Logger *zap.Logger

	TLSConfig     *tls.Config
	SeedMgmtAddrs []string
	Authenticator Authenticator
	BucketName    string

	ConfigPollerConfig ConfigPollerConfig
	StreamOptions      DcpStreamOptions
}

type dcpAgentState struct {
	bucket        string
	tlsConfig     *tls.Config
	authenticator Authenticator
	httpTransport *http.Transport

	latestConfig *ParsedConfig

	defStreamSet *DcpAgentStreamSet
}

type DcpAgent struct {
	logger      *zap.Logger
	networkType string

	lock  sync.Mutex
	state dcpAgentState

	cfgWatcher       *ConfigWatcherHttp
	cfgWatcherCancel func()

	vbRouter      VbucketRouter
	collections   CollectionResolver
	clientMgr     DcpClientManager
	dcpEvtHandler *DcpStreamRouterStatic

	mgmt *MgmtComponent
}

func CreateDcpAgent(ctx context.Context, opts DcpAgentOptions) (*DcpAgent, error) {
	if opts.BucketName == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	logger := loggerOrNop(opts.Logger)

	// We namespace the agent to improve debugging,
	logger = logger.With(
		zap.String("dcpAgentId", uuid.NewString()[:8]),
	)

	logger.Debug("Creating new dcp agent",
		zap.Any("config", opts),
		zap.String("build-version", buildVersion))

	clientName := fmt.Sprintf("gocbcorex-dcp/%s", buildVersion)
	srcHTTPAddrs := makeSrcHTTPAddrs(opts.SeedMgmtAddrs, opts.TLSConfig)

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

	logger.Debug("dcp agent bootstrapped",
		zap.Any("bootstrapConfig", bootstrapConfig),
		zap.String("networkType", networkType))

	agent := &DcpAgent{
		logger:      logger,
		networkType: networkType,

		state: dcpAgentState{
			bucket:        opts.BucketName,
			tlsConfig:     opts.TLSConfig,
			authenticator: opts.Authenticator,
			latestConfig:  bootstrapConfig,
			httpTransport: httpTransport,
		},
	}

	agentComponentConfigs := agent.genAgentComponentConfigsLocked()

	defRetries := NewRetryManagerDefault()

	agent.mgmt = NewMgmtComponent(
		defRetries,
		&agentComponentConfigs.MgmtComponentConfig,
		&MgmtComponentOptions{
			Logger:    logger,
			UserAgent: clientName,
		},
	)

	coreCollections, err := NewCollectionResolverHttp(&CollectionResolverHttpOptions{
		Logger:        agent.logger,
		BucketName:    opts.BucketName,
		MgmtComponent: agent.mgmt,
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

	vbRouter := NewVbucketRouter(&VbucketRouterOptions{
		Logger: agent.logger,
	})
	vbRouter.UpdateRoutingInfo(agentComponentConfigs.VbucketRoutingInfo)
	agent.vbRouter = vbRouter

	dcpEvtHandler := &DcpStreamRouterStatic{
		ClientHandlers: DcpEventsHandlers{},
	}
	agent.dcpEvtHandler = dcpEvtHandler

	clientMgr, err := NewDcpClientManager(&DcpClientManagerConfig{
		Clients: agentComponentConfigs.DcpClientManagerClients,
	}, &DcpClientManagerOptions{
		Logger:        agent.logger.Named("dcp-client-manager"),
		BucketName:    opts.BucketName,
		StreamOptions: opts.StreamOptions,
		Handlers:      dcpEvtHandler.Handlers(),
	})
	if err != nil {
		return nil, handleAgentCreateErr(err)
	}
	agent.clientMgr = clientMgr

	configWatcher, err := NewConfigWatcherHttp(
		&agentComponentConfigs.ConfigWatcherHttpConfig,
		&ConfigWatcherHttpOptions{
			Logger: logger.Named("http-config-watcher"),
		})
	if err != nil {
		return nil, handleAgentCreateErr(err)
	}

	agent.cfgWatcher = configWatcher
	agent.startConfigWatcher()

	return agent, nil
}

func (agent *DcpAgent) Close() error {
	err := agent.clientMgr.Close()
	if err != nil {
		agent.logger.Debug("Failed to close conn mgr", zap.Error(err))
	}

	agent.cfgWatcherCancel()
	agent.state.httpTransport.CloseIdleConnections()
	return nil
}

type NewStreamSetOptions struct {
	Handlers DcpEventsHandlers
}

func (agent *DcpAgent) NewStreamSet(
	opts *NewStreamSetOptions,
) (*DcpAgentStreamSet, error) {
	agent.lock.Lock()
	defer agent.lock.Unlock()

	if agent.state.defStreamSet != nil {
		return nil, fmt.Errorf("multiple stream sets is not currently supported")
	}

	agent.dcpEvtHandler.ClientHandlers = opts.Handlers

	streamSet, err := newDcpStreamSet(
		0,
		agent.vbRouter,
		agent.collections,
		agent.clientMgr,
		opts.Handlers.StreamOpen)
	if err != nil {
		return nil, err
	}

	agent.state.defStreamSet = streamSet
	return streamSet, nil
}

func (agent *DcpAgent) KeyToVbucket(key []byte) uint16 {
	agent.lock.Lock()
	defer agent.lock.Unlock()

	return agent.state.latestConfig.VbucketMap.VbucketByKey(key)
}

func (agent *DcpAgent) GetCollectionId(
	ctx context.Context,
	scopeName string,
	collectionName string,
) (collectionId uint32, manifestRev uint64, err error) {
	return agent.collections.ResolveCollectionID(ctx, scopeName, collectionName)
}

func (agent *DcpAgent) NumVbuckets() uint16 {
	agent.lock.Lock()
	defer agent.lock.Unlock()

	return uint16(agent.state.latestConfig.VbucketMap.NumVbuckets())
}

func (agent *DcpAgent) genAgentComponentConfigsLocked() *AgentComponentConfigs {
	return GenerateComponentConfigsFromConfig(
		agent.state.latestConfig,
		agent.networkType,
		agent.state.tlsConfig,
		agent.state.bucket,
		agent.state.authenticator,
		agent.state.httpTransport)
}

func (agent *DcpAgent) applyConfig(config *ParsedConfig) {
	agent.lock.Lock()
	defer agent.lock.Unlock()

	if !canUpdateConfig(config, agent.state.latestConfig, agent.logger) {
		return
	}

	agent.logger.Info("applying updated config",
		zap.Any("config", *config))

	agent.state.latestConfig = config
	agent.updateStateLocked()
}

func (agent *DcpAgent) updateStateLocked() {
	agent.logger.Debug("updating dcp components",
		zap.Any("state", agent.state),
		zap.Any("config", *agent.state.latestConfig))

	agentComponentConfigs := agent.genAgentComponentConfigsLocked()

	agent.vbRouter.UpdateRoutingInfo(agentComponentConfigs.VbucketRoutingInfo)

	err := agent.mgmt.Reconfigure(&agentComponentConfigs.MgmtComponentConfig)
	if err != nil {
		agent.logger.Error("failed to reconfigure management component", zap.Error(err))
	}

	if agent.cfgWatcher != nil {
		err := agent.cfgWatcher.Reconfigure(&agentComponentConfigs.ConfigWatcherHttpConfig)
		if err != nil {
			agent.logger.Error("failed to reconfigure http config watcher component", zap.Error(err))
		}
	}
}

func (agent *DcpAgent) startConfigWatcher() {
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
