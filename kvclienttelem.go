package gocbcorex

import (
	"context"
	"net"
	"time"

	"github.com/couchbase/gocbcorex/contrib/atomiccowcache"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type kvClientTelem struct {
	localHost  string
	localPort  int
	remoteHost string
	remotePort int

	durationMetric metric.Float64Histogram
	attribsCache   *atomiccowcache.Cache[kvClientTelemOpKey, attribute.Set]
}

var _ MemdClientTelem = (*kvClientTelem)(nil)

type kvClientTelemOpKey struct {
	bucketName string
	opName     string
}

func (k kvClientTelemOpKey) String() string { return k.bucketName + ":" + k.opName }

func newKvClientTelem(localAddr net.Addr, remoteAddr net.Addr) *kvClientTelem {
	localHost, localPort := hostPortFromNetAddr(localAddr)
	remoteHost, remotePort := hostPortFromNetAddr(remoteAddr)

	durationMetric, _ := meter.Float64Histogram("db.client.operation.duration",
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10))

	attribsCache := atomiccowcache.NewCache(
		func(k kvClientTelemOpKey) attribute.Set {
			return attribute.NewSet(
				semconv.DBSystemCouchbase,
				semconv.ServerAddress(remoteHost),
				semconv.ServerPort(remotePort),
				semconv.NetworkPeerAddress(localHost),
				semconv.NetworkPeerPort(localPort),
				semconv.DBNamespace(k.bucketName),
				semconv.DBOperationName(k.opName),
			)
		})

	return &kvClientTelem{
		localHost:      localHost,
		localPort:      localPort,
		remoteHost:     remoteHost,
		remotePort:     remotePort,
		durationMetric: durationMetric,
		attribsCache:   attribsCache,
	}
}

type kvClientTelemOp struct {
	parent *kvClientTelem

	startTime  time.Time
	bucketName string
	opName     string
	span       trace.Span
}

func (k *kvClientTelem) BeginOp(
	ctx context.Context,
	bucketName string,
	opName string,
) (context.Context, MemdClientTelemOp) {
	startTime := time.Now()

	ctx, span := tracer.Start(ctx, "memcached/"+opName,
		trace.WithSpanKind(trace.SpanKindClient))
	if span.IsRecording() {
		span.SetAttributes(
			semconv.ServerAddress(k.remoteHost),
			semconv.ServerPort(k.remotePort),
			semconv.NetworkPeerAddress(k.localHost),
			semconv.NetworkPeerPort(k.localPort),
			semconv.RPCMethod(opName),
			semconv.RPCSystemKey.String("memcached"))
	}

	return ctx, &kvClientTelemOp{
		parent:     k,
		startTime:  startTime,
		span:       span,
		bucketName: bucketName,
		opName:     opName,
	}
}

func (k *kvClientTelemOp) IsRecording() bool {
	return k.span.IsRecording()
}

func (k *kvClientTelemOp) MarkSent() {
	k.span.AddEvent("SENT")
}

func (k *kvClientTelemOp) MarkReceived() {
	k.span.AddEvent("RECEIVED")
}

func (k *kvClientTelemOp) RecordServerDuration(d time.Duration) {
	if k.span.IsRecording() {
		k.span.SetAttributes(attribute.Int("db.couchbase.server_duration",
			int(d/time.Microsecond)))
	}
}

func (k *kvClientTelemOp) recordDurationMetric(ctx context.Context, d time.Duration) {
	switch otel.GetMeterProvider().(type) {
	case metricnoop.MeterProvider:
		return
	}

	attribs := k.parent.attribsCache.Get(kvClientTelemOpKey{
		bucketName: k.bucketName,
		opName:     k.opName,
	})

	dtimeSecs := float64(d) / float64(time.Second)
	k.parent.durationMetric.Record(ctx, dtimeSecs, metric.WithAttributeSet(attribs))
}

func (k *kvClientTelemOp) End(ctx context.Context, err error) {
	etime := time.Now()
	dtime := etime.Sub(k.startTime)

	if err != nil {
		k.span.RecordError(err)
	}
	k.span.End()

	// we dont record the metric if the context has been cancelled in any
	// way, since its not representative of the actual time taken...
	if ctx.Err() == nil {
		k.recordDurationMetric(ctx, dtime)
	}
}
