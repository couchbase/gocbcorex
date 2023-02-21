package gocbcorex

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
	vbMap          *VbucketMap

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

func (config *routeConfig) IsVersioned() bool {
	return config.revEpoch > 0 || config.revID > 0
}

func (config *routeConfig) Compare(oconfig *routeConfig) int {
	if config.revEpoch < oconfig.revEpoch {
		// this config is an older epoch
		return -2
	} else if config.revEpoch > oconfig.revEpoch {
		// this config is a newer epoch
		return +2
	}

	if config.revID < oconfig.revID {
		// this config is an older config
		return -1
	} else if config.revID > oconfig.revID {
		// this config is a newer config
		return +1
	}

	return 0
}
