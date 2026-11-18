package gocbcorex

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/zap"
)

// connCountRecorder sums the values added to the connection count metric,
// so tests can check that every increment has a matching decrement.
type connCountRecorder struct {
	metricnoop.Int64UpDownCounter

	lock sync.Mutex
	sum  int64
}

func (r *connCountRecorder) Add(_ context.Context, value int64, _ ...metric.AddOption) {
	r.lock.Lock()
	r.sum += value
	r.lock.Unlock()
}

func (r *connCountRecorder) Sum() int64 {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.sum
}

type connCountMeter struct {
	metricnoop.Meter
	recorder *connCountRecorder
}

func (m connCountMeter) Int64UpDownCounter(
	name string,
	opts ...metric.Int64UpDownCounterOption,
) (metric.Int64UpDownCounter, error) {
	if name == semconv.DBClientConnectionCountName {
		return m.recorder, nil
	}
	return m.Meter.Int64UpDownCounter(name, opts...)
}

type connCountMeterProvider struct {
	metricnoop.MeterProvider
	meter connCountMeter
}

func (p connCountMeterProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return p.meter
}

var (
	connCountRecorderOnce sync.Once
	globalConnCountRecord *connCountRecorder
)

// installConnCountRecorder routes the package's meter to a recorder. The
// global meter provider only delegates once, so this is shared by all tests.
func installConnCountRecorder() *connCountRecorder {
	connCountRecorderOnce.Do(func() {
		globalConnCountRecord = &connCountRecorder{}
		otel.SetMeterProvider(connCountMeterProvider{
			meter: connCountMeter{recorder: globalConnCountRecord},
		})
	})
	return globalConnCountRecord
}

func newConnCountTestClient(
	t *testing.T,
	onDial func(clientOpts *memdx.ClientOptions),
) (KvClient, error) {
	logger, _ := zap.NewDevelopment()
	memdxCli := &MemdxClientMock{
		LocalAddrFunc:  func() net.Addr { return &net.TCPAddr{} },
		RemoteAddrFunc: func() net.Addr { return &net.TCPAddr{} },
		CloseFunc:      func() error { return nil },
	}

	return NewKvClient(context.Background(), &KvClientOptions{
		Logger: logger,
		DialMemdxClient: func(
			ctx context.Context,
			address string,
			dialOpts *memdx.DialConnOptions,
			clientOpts *memdx.ClientOptions,
		) (MemdxClient, error) {
			if onDial != nil {
				onDial(clientOpts)
			}
			return memdxCli, nil
		},
		Address: "endpoint1",
		BootstrapOpts: KvClientBootstrapOptions{
			DisableBootstrap:       true,
			DisableDefaultFeatures: true,
			DisableErrorMap:        true,
		},
	})
}

func TestKvClientConnCountOpenClose(t *testing.T) {
	recorder := installConnCountRecorder()
	start := recorder.Sum()

	cli, err := newConnCountTestClient(t, nil)
	require.NoError(t, err)
	require.Equal(t, start+1, recorder.Sum())

	require.NoError(t, cli.Close())
	require.Equal(t, start, recorder.Sum())
}
