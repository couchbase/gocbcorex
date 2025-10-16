package cbauthx

import (
	"github.com/couchbase/gocbcorex/contrib/buildversion"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	buildVersion string = buildversion.GetVersion("github.com/couchbase/gocbcorex")
	meter               = otel.Meter("github.com/couchbase/gocbcorex/cbauthx",
		metric.WithInstrumentationVersion(buildVersion))
)

var (
	authCheckLatencies, _ = meter.Float64Histogram("cbauth.authcheck.duration",
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10))
	certCheckLatencies, _ = meter.Float64Histogram("cbauth.certcheck.duration",
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10))
	revrpcHeartbeats, _ = meter.Int64Counter("cbauth.revrpc.total_heartbeats")
	revrpcDbUpdates, _  = meter.Int64Counter("cbauth.revrpc.total_dbupdates")
)
