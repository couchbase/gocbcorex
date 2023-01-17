package core

import "log"

type bucketType int

const (
	bktTypeNone                 = -1
	bktTypeInvalid   bucketType = 0
	bktTypeCouchbase            = iota
)

type routeEndpoints struct {
	SSLEndpoints    []string
	NonSSLEndpoints []string
}

type routeConfig struct {
	revID          int64
	revEpoch       int64
	uuid           string
	name           string
	bktType        bucketType
	kvServerList   routeEndpoints
	capiEpList     routeEndpoints
	mgmtEpList     routeEndpoints
	n1qlEpList     routeEndpoints
	ftsEpList      routeEndpoints
	cbasEpList     routeEndpoints
	eventingEpList routeEndpoints
	gsiEpList      routeEndpoints
	backupEpList   routeEndpoints
	vbMap          *vbucketMap

	clusterCapabilitiesVer []int
	clusterCapabilities    map[string][]string

	bucketCapabilities    []string
	bucketCapabilitiesVer string
}

func (config *routeConfig) IsValid() bool {
	if (len(config.kvServerList.SSLEndpoints) == 0 || len(config.mgmtEpList.SSLEndpoints) == 0) &&
		(len(config.kvServerList.NonSSLEndpoints) == 0 || len(config.mgmtEpList.NonSSLEndpoints) == 0) {
		return false
	}
	switch config.bktType {
	case bktTypeCouchbase:
		return config.vbMap != nil && config.vbMap.IsValid()
	// case bktTypeMemcached:
	// 	return config.ketamaMap != nil && config.ketamaMap.IsValid()
	case bktTypeNone:
		return true
	default:
		return false
	}
}

func (config *routeConfig) IsNewerThan(oldCfg *routeConfig) bool {
	if config.revEpoch < oldCfg.revEpoch {
		log.Printf("Ignoring new configuration as it has an older revision epoch")
		return false
	} else if config.revEpoch == oldCfg.revEpoch {
		if config.revID == 0 {
			log.Printf("Unversioned configuration data, switching.")
		} else if config.revID == oldCfg.revID {
			log.Printf("Ignoring configuration with identical revision number")
			return false
		} else if config.revID < oldCfg.revID {
			log.Printf("Ignoring new configuration as it has an older revision id")
			return false
		}
	}

	return true
}
