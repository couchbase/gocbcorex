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
	optimalMemdRequests, _    = meter.Int64Counter("gocbcorex.optimal_memd_requests")
	suboptimalMemdRequests, _ = meter.Int64Counter("gocbcorex.suboptimal_memd_requests")
)
