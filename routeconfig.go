package core

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
