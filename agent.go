package core

import (
	"context"
	"crypto/tls"
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
	configMgr   *RouteConfigManager
	connMgr     KvClientManager
	collections CollectionResolver
	retries     RetryManager
	vbs         *vbucketRouter

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
		configMgr: newConfigManager(),
		retries:   NewRetryManagerFastFail(),
	}

	clients := make(map[string]*KvClientConfig)
	for _, addr := range opts.MemdAddrs {
		clients[addr] = &KvClientConfig{
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

	agent.vbs = newVbucketRouter()

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
		vbs:         agent.vbs,
	}

	return agent, nil
}

func (agent *Agent) updateStateLocked() {
	log.Printf("updating config: %+v %+v", agent.state, *agent.state.latestConfig)
	routeCfg := agent.state.latestConfig

	var memdTlsConfig *tls.Config
	var memdList []string
	var mgmtList []string
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
	for _, addr := range memdList {
		clients[addr] = &KvClientConfig{
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

	agent.vbs.UpdateRoutingInfo(&vbucketRoutingInfo{
		vbmap:      routeCfg.vbMap,
		serverList: memdList,
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
