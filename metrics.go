package gocbcorex

import (
	"go.opentelemetry.io/otel"
)

var (
	meter = otel.Meter("github.com/couchbase/gocbcorex")
)
