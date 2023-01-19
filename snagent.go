package core

import (
	"context"
	"crypto/tls"
	"github.com/couchbase/stellar-nebula/core/memdx"
	"net/http"
	"time"
)

type FakeAgent struct {
	bucket    string
	tlsConfig *tls.Config

	poller      ConfigPoller
	configMgr   ConfigManager
	vbuckets    VbucketDispatcher
	connMgr     ConnectionManager
	crud        *CrudComponent
	collections CollectionResolver
	retries     RetryComponent
}

func CreateAgent(opts FakeAgentOptions) *FakeAgent {
	agent := &FakeAgent{
		bucket:    opts.BucketName,
		tlsConfig: opts.TLSConfig,

		poller: newhttpConfigPoller(opts.HTTPAddrs, httpPollerProperties{
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
		connMgr: newConnectionManager(connManagerOptions{
			ConnectionsPerNode: 1,
			TlsConfig:          nil,
			SelectedBucket:     opts.BucketName,
			Features: []memdx.HelloFeature{
				memdx.HelloFeatureTLS, memdx.HelloFeatureXattr, memdx.HelloFeatureSelectBucket, memdx.HelloFeatureXerror,
				memdx.HelloFeatureJSON, memdx.HelloFeatureSeqNo, memdx.HelloFeatureSnappy, memdx.HelloFeatureDurations,
				memdx.HelloFeatureCollections, memdx.HelloFeatureUnorderedExec, memdx.HelloFeatureAltRequests,
				memdx.HelloFeatureCreateAsDeleted, memdx.HelloFeatureReplaceBodyWithXattr, memdx.HelloFeaturePITR,
				memdx.HelloFeatureSyncReplication,
			},
			Username: opts.Username,
			Password: opts.Password,
		}),
		retries: newRetryComponent(),
	}

	agent.collections = newPerCidCollectionResolver(perCidCollectionResolverOptions{
		Dispatcher: agent.connMgr,
	})

	agent.crud = &CrudComponent{
		collections: agent.collections,
		vbuckets:    agent.vbuckets,
		retries:     agent.retries,
		// errorResolver: new,
		connManager: agent.connMgr,
	}

	go agent.WatchConfigs()

	return agent
}

func (agent *FakeAgent) WatchConfigs() {
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

		agent.connMgr.UpdateEndpoints(serverList)
		agent.vbuckets.StoreVbucketRoutingInfo(&vbucketRoutingInfo{
			vbmap:      routeCfg.vbMap,
			serverList: serverList,
		})
		agent.poller.UpdateEndpoints(mgmtList)
	}
}
