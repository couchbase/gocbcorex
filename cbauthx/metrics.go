package cbauthx

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter = otel.Meter("github.com/couchbase/gocbcorex/cbauthx")
)

var (
	authCheckLatencies, _ = meter.Float64Histogram("cbauth.authcheck.duration",
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10))
	revrpcHeartbeats, _ = meter.Int64Counter("cbauth.revrpc.total_heartbeats")
	revrpcDbUpdates, _  = meter.Int64Counter("cbauth.revrpc.total_dbupdates")
)
