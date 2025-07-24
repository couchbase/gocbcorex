package gocbcorex

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter = otel.Meter("github.com/couchbase/gocbcorex",
		metric.WithInstrumentationVersion(buildVersion))
)
