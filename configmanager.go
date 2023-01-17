package core

import (
	"fmt"
	"log"
	"strings"

	"github.com/couchbase/stellar-nebula/contrib/cbconfig"
)

type ConfigManager interface {
	ApplyConfig(sourceHostname string, json *cbconfig.TerseConfigJson) (*routeConfig, bool)
}

type configManager struct {
	currentConfig AtomicPointer[routeConfig]
}

func (cm *configManager) ApplyConfig(sourceHostname string, cfg *cbconfig.TerseConfigJson) (*routeConfig, bool) {
	for {
		oldConfig := cm.currentConfig.Load()

		newConfig := cm.applyConfig(sourceHostname, cfg)
		if !newConfig.IsValid() {
			return nil, false
		}

		if oldConfig != nil {
			// Check some basic things to ensure consistency!
			// If oldCfg name was empty and the new cfg isn't then we're moving from cluster to bucket connection.
			if oldConfig.revID > -1 && (oldConfig.name != "" && newConfig.name != "") {
				if (newConfig.vbMap == nil) != (oldConfig.vbMap == nil) {
					log.Printf("Received a configuration with a different number of vbuckets %s-%s.  Ignoring.", oldConfig.name, newConfig.name)
					return nil, false
				}

				if newConfig.vbMap != nil && newConfig.vbMap.NumVbuckets() != oldConfig.vbMap.NumVbuckets() {
					log.Printf("Received a configuration with a different number of vbuckets %s-%s.  Ignoring.", oldConfig.name, newConfig.name)
					return nil, false
				}
			}

			// Check that the new config data is newer than the current one, in the case where we've done a select bucket
			// against an existing connection then the revisions could be the same. In that case the configuration still
			// needs to be applied.
			// In the case where the rev epochs are the same then we need to compare rev IDs. If the new config epoch is lower
			// than the old one then we ignore it, if it's newer then we apply the new config.
			if newConfig.bktType != oldConfig.bktType {
				fmt.Printf("Configuration data changed bucket type, switching.")
			} else if !newConfig.IsNewerThan(oldConfig) {
				return nil, false
			}
		}

		if cm.currentConfig.CompareAndSwap(oldConfig, newConfig) {
			return newConfig, true
		} else if oldConfig == nil {
			// This would be odd.
			return nil, false
		}
	}
}

func (cm *configManager) applyConfig(sourceHostname string, cfg *cbconfig.TerseConfigJson) *routeConfig {
	var (
		kvServerList   = routeEndpoints{}
		capiEpList     = routeEndpoints{}
		mgmtEpList     = routeEndpoints{}
		n1qlEpList     = routeEndpoints{}
		ftsEpList      = routeEndpoints{}
		cbasEpList     = routeEndpoints{}
		eventingEpList = routeEndpoints{}
		gsiEpList      = routeEndpoints{}
		backupEpList   = routeEndpoints{}
		bktType        bucketType
	)

	switch cfg.NodeLocator {
	// case "ketama":
	// 	bktType = bktTypeMemcached
	case "vbucket":
		bktType = bktTypeCouchbase
	default:
		if cfg.UUID == "" {
			bktType = bktTypeNone
		} else {
			bktType = bktTypeInvalid
		}
	}

	lenNodes := len(cfg.Nodes)
	for i, node := range cfg.NodesExt {
		hostname := getHostname(node.Hostname, sourceHostname)
		ports := node.Services

		// if networkType != "default" {
		// 	if altAddr, ok := node.AltAddresses[networkType]; ok {
		// 		hostname = altAddr.Hostname
		// 		if altAddr.Ports != nil {
		// 			ports = *altAddr.Ports
		// 		}
		// 	} else {
		// 		if !firstConnect {
		// 			logDebugf("Invalid config network type %s", networkType)
		// 		}
		// 		continue
		// 	}
		// }

		endpoints := endpointsFromPorts(ports, hostname)
		if endpoints.kvServer != "" {
			if bktType > bktTypeInvalid && i >= lenNodes {
				// logDebugf("KV node present in nodesext but not in nodes for %s", endpoints.kvServer)
			} else {
				kvServerList.NonSSLEndpoints = append(kvServerList.NonSSLEndpoints, endpoints.kvServer)
			}
		}
		if endpoints.capiEp != "" {
			capiEpList.NonSSLEndpoints = append(capiEpList.NonSSLEndpoints, endpoints.capiEp)
		}
		if endpoints.mgmtEp != "" {
			mgmtEpList.NonSSLEndpoints = append(mgmtEpList.NonSSLEndpoints, endpoints.mgmtEp)
		}
		if endpoints.n1qlEp != "" {
			n1qlEpList.NonSSLEndpoints = append(n1qlEpList.NonSSLEndpoints, endpoints.n1qlEp)
		}
		if endpoints.ftsEp != "" {
			ftsEpList.NonSSLEndpoints = append(ftsEpList.NonSSLEndpoints, endpoints.ftsEp)
		}
		if endpoints.cbasEp != "" {
			cbasEpList.NonSSLEndpoints = append(cbasEpList.NonSSLEndpoints, endpoints.cbasEp)
		}
		if endpoints.eventingEp != "" {
			eventingEpList.NonSSLEndpoints = append(eventingEpList.NonSSLEndpoints, endpoints.eventingEp)
		}
		if endpoints.gsiEp != "" {
			gsiEpList.NonSSLEndpoints = append(gsiEpList.NonSSLEndpoints, endpoints.gsiEp)
		}
		if endpoints.backupEp != "" {
			backupEpList.NonSSLEndpoints = append(backupEpList.NonSSLEndpoints, endpoints.backupEp)
		}

		if endpoints.kvServerSSL != "" {
			if bktType > bktTypeInvalid && i >= lenNodes {
				// logDebugf("KV node present in nodesext but not in nodes for %s", endpoints.kvServerSSL)
			} else {
				kvServerList.SSLEndpoints = append(kvServerList.SSLEndpoints, endpoints.kvServerSSL)
			}
		}
		if endpoints.capiEpSSL != "" {
			capiEpList.SSLEndpoints = append(capiEpList.SSLEndpoints, endpoints.capiEpSSL)
		}
		if endpoints.mgmtEpSSL != "" {
			mgmtEpList.SSLEndpoints = append(mgmtEpList.SSLEndpoints, endpoints.mgmtEpSSL)
		}
		if endpoints.n1qlEpSSL != "" {
			n1qlEpList.SSLEndpoints = append(n1qlEpList.SSLEndpoints, endpoints.n1qlEpSSL)
		}
		if endpoints.ftsEpSSL != "" {
			ftsEpList.SSLEndpoints = append(ftsEpList.SSLEndpoints, endpoints.ftsEpSSL)
		}
		if endpoints.cbasEpSSL != "" {
			cbasEpList.SSLEndpoints = append(cbasEpList.SSLEndpoints, endpoints.cbasEpSSL)
		}
		if endpoints.eventingEpSSL != "" {
			eventingEpList.SSLEndpoints = append(eventingEpList.SSLEndpoints, endpoints.eventingEpSSL)
		}
		if endpoints.gsiEpSSL != "" {
			gsiEpList.SSLEndpoints = append(gsiEpList.SSLEndpoints, endpoints.gsiEpSSL)
		}
		if endpoints.backupEpSSL != "" {
			backupEpList.SSLEndpoints = append(backupEpList.SSLEndpoints, endpoints.backupEpSSL)
		}
	}

	rc := &routeConfig{
		revID:                  int64(cfg.Rev),
		revEpoch:               int64(cfg.RevEpoch),
		uuid:                   cfg.UUID,
		name:                   cfg.Name,
		kvServerList:           kvServerList,
		capiEpList:             capiEpList,
		mgmtEpList:             mgmtEpList,
		n1qlEpList:             n1qlEpList,
		ftsEpList:              ftsEpList,
		cbasEpList:             cbasEpList,
		eventingEpList:         eventingEpList,
		gsiEpList:              gsiEpList,
		backupEpList:           backupEpList,
		bktType:                bktType,
		clusterCapabilities:    cfg.ClusterCapabilities,
		clusterCapabilitiesVer: cfg.ClusterCapabilitiesVer,
		bucketCapabilities:     cfg.BucketCapabilities,
		bucketCapabilitiesVer:  cfg.BucketCapabilitiesVer,
	}

	if bktType == bktTypeCouchbase {
		vbMap := cfg.VBucketServerMap.VBucketMap
		numReplicas := cfg.VBucketServerMap.NumReplicas
		rc.vbMap = newVbucketMap(vbMap, numReplicas)
	}
	// } else if bktType == bktTypeMemcached {
	// 	var endpoints []routeEndpoint
	// 	if useSsl {
	// 		endpoints = kvServerList.SSLEndpoints
	// 	} else {
	// 		endpoints = kvServerList.NonSSLEndpoints
	// 	}
	// 	rc.ketamaMap = newKetamaContinuum(endpoints)
	// }

	return rc
}

func getHostname(hostname, sourceHostname string) string {
	// Hostname blank means to use the same one as was connected to
	if hostname == "" {
		// Note that the SourceHostname will already be IPv6 wrapped
		hostname = sourceHostname
	} else {
		// We need to detect an IPv6 address here and wrap it in the appropriate
		// [] block to indicate its IPv6 for the rest of the system.
		if strings.Contains(hostname, ":") {
			hostname = "[" + hostname + "]"
		}
	}

	return hostname
}

type serverEps struct {
	kvServerSSL   string
	capiEpSSL     string
	mgmtEpSSL     string
	n1qlEpSSL     string
	ftsEpSSL      string
	cbasEpSSL     string
	eventingEpSSL string
	gsiEpSSL      string
	backupEpSSL   string
	kvServer      string
	capiEp        string
	mgmtEp        string
	n1qlEp        string
	ftsEp         string
	cbasEp        string
	eventingEp    string
	gsiEp         string
	backupEp      string
}

func endpointsFromPorts(ports map[string]int, hostname string) *serverEps {
	lists := &serverEps{}

	if ports["kvSSL"] > 0 {
		lists.kvServerSSL = fmt.Sprintf("couchbases://%s:%d", hostname, ports["kvSSL"])
	}
	if ports["capiSSL"] > 0 {
		lists.capiEpSSL = fmt.Sprintf("https://%s:%d", hostname, ports["capiSSL"])
	}
	if ports["mgmtSSL"] > 0 {
		lists.mgmtEpSSL = fmt.Sprintf("https://%s:%d", hostname, ports["mgmtSSL"])
	}
	if ports["n1qlSSL"] > 0 {
		lists.n1qlEpSSL = fmt.Sprintf("https://%s:%d", hostname, ports["n1qlSSL"])
	}
	if ports["ftsSSL"] > 0 {
		lists.ftsEpSSL = fmt.Sprintf("https://%s:%d", hostname, ports["ftsSSL"])
	}
	if ports["cbasSSL"] > 0 {
		lists.cbasEpSSL = fmt.Sprintf("https://%s:%d", hostname, ports["cbasSSL"])
	}
	if ports["eventingSSL"] > 0 {
		lists.eventingEpSSL = fmt.Sprintf("https://%s:%d", hostname, ports["eventingSSL"])
	}
	if ports["indexHttps"] > 0 {
		lists.gsiEpSSL = fmt.Sprintf("https://%s:%d", hostname, ports["indexHttps"])
	}
	if ports["backupSSL"] > 0 {
		lists.backupEpSSL = fmt.Sprintf("https://%s:%d", hostname, ports["backupSSL"])
	}

	if ports["kv"] > 0 {
		lists.kvServer = fmt.Sprintf("couchbase://%s:%d", hostname, ports["kv"])
	}
	if ports["capi"] > 0 {
		lists.capiEp = fmt.Sprintf("http://%s:%d", hostname, ports["capi"])
	}
	if ports["mgmt"] > 0 {
		lists.mgmtEp = fmt.Sprintf("http://%s:%d", hostname, ports["mgmt"])
	}
	if ports["n1ql"] > 0 {
		lists.n1qlEp = fmt.Sprintf("http://%s:%d", hostname, ports["n1ql"])
	}
	if ports["fts"] > 0 {
		lists.ftsEp = fmt.Sprintf("http://%s:%d", hostname, ports["fts"])
	}
	if ports["cbas"] > 0 {
		lists.cbasEp = fmt.Sprintf("http://%s:%d", hostname, ports["cbas"])
	}
	if ports["eventing"] > 0 {
		lists.eventingEp = fmt.Sprintf("http://%s:%d", hostname, ports["eventing"])
	}
	if ports["indexHttp"] > 0 {
		lists.gsiEp = fmt.Sprintf("http://%s:%d", hostname, ports["indexHttp"])
	}
	if ports["backup"] > 0 {
		lists.backupEp = fmt.Sprintf("http://%s:%d", hostname, ports["backup"])
	}
	return lists
}
