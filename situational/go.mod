module github.com/couchbase/gocbcorex/situational

go 1.21.4

require (
	github.com/couchbase/gocbcorex v0.0.0-20231123235747-3265df2f19d2
	github.com/couchbaselabs/cbdinocluster v0.0.22
	go.uber.org/zap v1.24.0
	gopkg.in/yaml.v3 v3.0.1
)

replace github.com/couchbase/gocbcorex => ../

require (
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	go.opentelemetry.io/otel v1.16.0 // indirect
	go.opentelemetry.io/otel/metric v1.16.0 // indirect
	go.opentelemetry.io/otel/trace v1.16.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/exp v0.0.0-20230713183714-613f0c0eb8a1 // indirect
)
