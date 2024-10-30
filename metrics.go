package gocbcorex

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter = otel.Meter("github.com/couchbase/gocbcorex")
)

var (
	clientOpDurationMetric, _ = meter.Float64Histogram("db.client.operation.duration",
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10))
)
