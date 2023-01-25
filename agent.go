package core

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"
)

type Agent struct {
	bucket    string
	tlsConfig *tls.Config
	username  string
	password  string

	poller      ConfigPoller
	configMgr   ConfigManager
	vbuckets    VbucketDispatcher
	connMgr     NodeKvClientProvider
	collections CollectionResolver
	retries     RetryManager

	crud *CrudComponent
}

func CreateAgent(opts AgentOptions) (*Agent, error) {
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
		bucket:    opts.BucketName,
		tlsConfig: opts.TLSConfig,
		username:  opts.Username,
		password:  opts.Password,

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
		vbuckets:  newVbucketDispatcher(),
		retries:   NewRetryManagerDefault(),
	}

	var err error
	agent.connMgr, err = NewNodeKvClientManager(&NodeKvClientManagerOptions{
		Endpoints:      opts.MemdAddrs,
		TLSConfig:      agent.tlsConfig,
		SelectedBucket: opts.BucketName,
		Username:       opts.Username,
		Password:       opts.Password,
	})
	if err != nil {
		return nil, err
	}

	agent.collections, err = NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver: &CollectionResolverMemd{
			connMgr: agent.connMgr,
		},
		ResolveTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	agent.crud = &CrudComponent{
		collections: agent.collections,
		vbuckets:    agent.vbuckets,
		retries:     agent.retries,
		// errorResolver: new,
		connManager: agent.connMgr,
	}

	go agent.WatchConfigs()

	return agent, nil
}

func (agent *Agent) WatchConfigs() {
	configCh, err := agent.poller.Watch(context.Background()) // TODO: this context probably needs to be linked with agent shutdown
	if err != nil {
		// TODO: Errr, panic?
		return
	}

	for config := range configCh {
		routeCfg, configOK := agent.configMgr.ApplyConfig(config.SourceHostname, config.Config)
		if !configOK {
			// Either the config is invalid or the config manager has already seen a config with an equal
			// or newer revision, so we wait for the next config.
			continue
		}

		var mgmtList []string
		var serverList []string
		if agent.tlsConfig == nil {
			serverList = make([]string, len(routeCfg.kvServerList.NonSSLEndpoints))
			copy(serverList, routeCfg.kvServerList.NonSSLEndpoints)
			mgmtList = make([]string, len(routeCfg.mgmtEpList.NonSSLEndpoints))
			copy(mgmtList, routeCfg.mgmtEpList.NonSSLEndpoints)
		} else {
			serverList = make([]string, len(routeCfg.kvServerList.SSLEndpoints))
			copy(serverList, routeCfg.kvServerList.SSLEndpoints)
			mgmtList = make([]string, len(routeCfg.mgmtEpList.SSLEndpoints))
			copy(mgmtList, routeCfg.mgmtEpList.SSLEndpoints)
		}

		agent.connMgr.Reconfigure(&NodeKvClientManagerOptions{
			Endpoints:      serverList,
			TLSConfig:      agent.tlsConfig,
			SelectedBucket: agent.bucket,
			Username:       agent.username,
			Password:       agent.password,
		})
		agent.vbuckets.StoreVbucketRoutingInfo(&vbucketRoutingInfo{
			vbmap:      routeCfg.vbMap,
			serverList: serverList,
		})
		agent.poller.UpdateEndpoints(mgmtList)
	}
}
