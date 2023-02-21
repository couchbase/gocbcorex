package gocbcorex

// ServiceType specifies a particular Couchbase service type.
type ServiceType int

const (
	// MemdService represents a memcached service.
	MemdService = ServiceType(1)

	// MgmtService represents a management service (typically ns_server).
	MgmtService = ServiceType(2)

	// QueryService represents a N1QL service (typically for query).
	QueryService = ServiceType(4)

	// SearchService represents a full-text-search service.
	SearchService = ServiceType(5)
)
