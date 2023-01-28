package core

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type agentState struct {
	bucket             string
	tlsConfig          *tls.Config
	username           string
	password           string
	numPoolConnections uint

	lastClients  map[string]*KvClientConfig
	latestConfig *routeConfig
}

type Agent struct {
	lock  sync.Mutex
	state agentState

	poller      ConfigPoller
	configMgr   ConfigManager
	connMgr     KvClientManager
	collections CollectionResolver
	retries     RetryManager
	vbRouter    VbucketRouter

	crud *CrudComponent
}

func CreateAgent(ctx context.Context, opts AgentOptions) (*Agent, error) {
	var srcHTTPAddrs []string
	for _, hostPort := range opts.HTTPAddrs {
		if opts.TLSConfig == nil {
			ep := fmt.Sprintf("http://%s", hostPort)
			srcHTTPAddrs = append(srcHTTPAddrs, ep)
		} else {
			ep := fmt.Sprintf("https://%s", hostPort)
			srcHTTPAddrs = append(srcHTTPAddrs, ep)
		}
	}

	agent := &Agent{
		state: agentState{
			bucket:             opts.BucketName,
			tlsConfig:          opts.TLSConfig,
			username:           opts.Username,
			password:           opts.Password,
			numPoolConnections: 1,
		},

		poller: newhttpConfigPoller(srcHTTPAddrs, httpPollerProperties{
			ConfHTTPRetryDelay:   10 * time.Second,
			ConfHTTPRedialPeriod: 10 * time.Second,
			ConfHTTPMaxWait:      5 * time.Second,
			HttpClient:           http.DefaultClient,
			BucketName:           opts.BucketName,
			Username:             opts.Username,
			Password:             opts.Password,
		}),
		configMgr: NewConfigManager(),
		retries:   NewRetryManagerFastFail(),
	}

	clients := make(map[string]*KvClientConfig)
	for addrIdx, addr := range opts.MemdAddrs {
		nodeId := fmt.Sprintf("bootstrap-%d", addrIdx)
		clients[nodeId] = &KvClientConfig{
			Address:        addr,
			TlsConfig:      agent.state.tlsConfig,
			SelectedBucket: agent.state.bucket,
			Username:       agent.state.username,
			Password:       agent.state.password,
		}
	}
	connMgr, err := NewKvClientManager(&KvClientManagerConfig{
		NumPoolConnections: agent.state.numPoolConnections,
		Clients:            clients,
	}, nil)
	if err != nil {
		return nil, err
	}
	agent.connMgr = connMgr

	collections, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver: &CollectionResolverMemd{
			connMgr: agent.connMgr,
		},
		ResolveTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	agent.collections = collections

	agent.vbRouter = newVbucketRouter()

	agent.configMgr.RegisterCallback(func(rc *routeConfig) {
		agent.lock.Lock()
		agent.state.latestConfig = rc
		agent.updateStateLocked()
		agent.lock.Unlock()
	})

	err = agent.startConfigWatcher(ctx)
	if err != nil {
		return nil, err
	}

	agent.crud = &CrudComponent{
		collections: agent.collections,
		retries:     agent.retries,
		// errorResolver: new,
		connManager: agent.connMgr,
		vbs:         agent.vbRouter,
	}

	return agent, nil
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
	agent.state.username = opts.Username
	agent.state.password = opts.Password
	agent.state.bucket = opts.BucketName
	agent.updateStateLocked()

	return nil
}

func (agent *Agent) updateStateLocked() {
	log.Printf("updating config: %+v %+v", agent.state, *agent.state.latestConfig)
	routeCfg := agent.state.latestConfig

	var memdTlsConfig *tls.Config
	var nodeNames []string
	var memdList []string
	var mgmtList []string

	nodeNames = make([]string, len(routeCfg.kvServerList.NonSSLEndpoints))
	for nodeIdx, addr := range routeCfg.kvServerList.NonSSLEndpoints {
		nodeNames[nodeIdx] = fmt.Sprintf("node@%s", addr)
	}

	if agent.state.tlsConfig == nil {
		memdList = make([]string, len(routeCfg.kvServerList.NonSSLEndpoints))
		copy(memdList, routeCfg.kvServerList.NonSSLEndpoints)
		memdTlsConfig = nil

		mgmtList = make([]string, len(routeCfg.mgmtEpList.NonSSLEndpoints))
		for epIdx, ep := range routeCfg.mgmtEpList.NonSSLEndpoints {
			mgmtList[epIdx] = "http://" + ep
		}
	} else {
		memdList = make([]string, len(routeCfg.kvServerList.SSLEndpoints))
		copy(memdList, routeCfg.kvServerList.SSLEndpoints)
		memdTlsConfig = agent.state.tlsConfig

		mgmtList = make([]string, len(routeCfg.mgmtEpList.NonSSLEndpoints))
		for epIdx, ep := range routeCfg.mgmtEpList.NonSSLEndpoints {
			mgmtList[epIdx] = "https://" + ep
		}
	}

	clients := make(map[string]*KvClientConfig)
	for addrIdx, addr := range memdList {
		nodeName := nodeNames[addrIdx]
		clients[nodeName] = &KvClientConfig{
			Address:        addr,
			TlsConfig:      memdTlsConfig,
			SelectedBucket: agent.state.bucket,
			Username:       agent.state.username,
			Password:       agent.state.password,
		}
	}

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
	for clientName, client := range clients {
		if oldClients[clientName] == nil {
			oldClients[clientName] = client
		}
	}

	agent.connMgr.Reconfigure(&KvClientManagerConfig{
		NumPoolConnections: agent.state.numPoolConnections,
		Clients:            oldClients,
	})

	agent.vbRouter.UpdateRoutingInfo(&VbucketRoutingInfo{
		VbMap:      routeCfg.vbMap,
		ServerList: nodeNames,
	})

	agent.connMgr.Reconfigure(&KvClientManagerConfig{
		NumPoolConnections: agent.state.numPoolConnections,
		Clients:            clients,
	})

	agent.poller.UpdateEndpoints(mgmtList)
}

func (agent *Agent) startConfigWatcher(ctx context.Context) error {
	configCh, err := agent.poller.Watch(ctx)
	if err != nil {
		return err
	}

	var firstConfig *TerseConfigJsonWithSource
	select {
	case config := <-configCh:
		firstConfig = config
	case <-ctx.Done():
		return ctx.Err()
	}

	agent.configMgr.ApplyConfig(firstConfig.SourceHostname, firstConfig.Config)

	go func() {
		for config := range configCh {
			agent.configMgr.ApplyConfig(config.SourceHostname, config.Config)
		}
	}()

	return nil
}
