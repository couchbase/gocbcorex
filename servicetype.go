package gocbcorex

import "encoding/hex"

// ServiceType specifies a particular Couchbase service type.
type ServiceType int

const (
	// ServiceTypeMemd represents a memcached service.
	ServiceTypeMemd = ServiceType(1)

	// ServiceTypeMgmt represents a management service (typically ns_server).
	ServiceTypeMgmt = ServiceType(2)

	// ServiceTypeQuery represents a N1QL service (typically for query).
	ServiceTypeQuery = ServiceType(4)

	// ServiceTypeSearch represents a full-text-search service.
	ServiceTypeSearch = ServiceType(5)

	// ServiceTypeAnalytics represents an analytics service.
	ServiceTypeAnalytics = ServiceType(6)
)

func (s ServiceType) String() string {
	switch s {
	case ServiceTypeMemd:
		return "Memd"
	case ServiceTypeMgmt:
		return "Mgmt"
	case ServiceTypeQuery:
		return "Query"
	case ServiceTypeSearch:
		return "Search"
	case ServiceTypeAnalytics:
		return "Analytics"
	}

	return "x" + hex.EncodeToString([]byte{byte(s)})
}
