package core

import "context"

type SNAgent struct {
	bucket string

	poller           ConfigPoller
	configMgr        ConfigManager
	vbuckets         VbucketDispatcher
	serverDispatcher ServerDispatcher
}

func Connect() *SNAgent {
	agent := &SNAgent{}

	go agent.WatchConfigs()

	return agent
}

func (agent *SNAgent) WatchConfigs() {
	configCh, err := agent.poller.Watch(context.Background(), agent.bucket) // TODO: this context probably needs to be linked with agent shutdown
	if err != nil {
		// TODO: Errr, panic?
		return
	}

	for config := range configCh {
		routeCfg, configOK := agent.configMgr.ApplyConfig(config)
		if !configOK {
			// Either the config is invalid or the config manager has already seen a config with an equal
			// or newer revision, so we wait for the next config.
			continue
		}

		serverList := routeCfg.kvServerList.SSLEndpoints // TODO: pick endpoints based on TLS config
		agent.vbuckets.StoreVbucketRoutingInfo(&vbucketRoutingInfo{
			vbmap:      routeCfg.vbMap,
			serverList: serverList,
		})
		agent.serverDispatcher.ApplyEndpoints(serverList)
	}
}