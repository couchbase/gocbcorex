package gocbcorex

import (
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("github.com/couchbase/gocbcorex")
