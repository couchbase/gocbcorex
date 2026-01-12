package gocbcorex

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter = otel.Meter("github.com/couchbase/gocbcorex",
		metric.WithInstrumentationVersion(buildVersion))
)

var (
	// localMemdRequests tracks the number of memd requests that can be serviced by a node running locally to this instance
	// of gocbcorex.
	localMemdRequests, _ = meter.Int64Counter("gocbcorex.local_memd_requests")

	// serverGroupNodeMemdRequests tracks the number of memd requests that can be serviced by a node in the same server group
	// as this instance of gocbcorex.
	serverGroupMemdRequests, _ = meter.Int64Counter("gocbcorex.server_group_memd_requests")

	// remoteMemdRequests tracks the number of memd requests that cannot be served by a local or a node in the same server group
	// as this gocbcorex instance. If no routing information is available then this will track all memd requests.
	remoteMemdRequests, _ = meter.Int64Counter("gocbcorex.remote_memd_requests")
)
