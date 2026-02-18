package cbqueryx_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func intBenchQueryEndpoint(b *testing.B) string {
	mgmtEndpoint := fmt.Sprintf("http://%s", testutilsint.TestOpts.HTTPAddrs[0])
	config, err := cbmgmtx.Management{
		Transport: http.DefaultTransport,
		UserAgent: "bench-agent",
		Endpoint:  mgmtEndpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
	}.GetTerseClusterConfig(context.Background(), &cbmgmtx.GetTerseClusterConfigOptions{})
	require.NoError(b, err)

	for _, nodeExt := range config.NodesExt {
		if nodeExt.Services.N1ql > 0 {
			return fmt.Sprintf("http://%s:%d", nodeExt.Hostname, nodeExt.Services.N1ql)
		}
	}

	b.Skip("skipping benchmark, no query nodes available")
	return ""
}

func intBenchQuery(b *testing.B) cbqueryx.Query {
	if !testutilsint.TestOpts.LongTest {
		b.Skipf("skipping long benchmark")
	}

	return cbqueryx.Query{
		Logger:    zap.NewNop(),
		Transport: http.DefaultTransport,
		UserAgent: "bench-agent",
		Endpoint:  intBenchQueryEndpoint(b),
		Auth: &cbhttpx.BasicAuth{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
	}
}

func drainRows(b *testing.B, res cbqueryx.ResultStream, iter int) {
	for res.HasMoreRows() {
		_, err := res.ReadRow()
		if err != nil {
			b.Fatalf("iteration %d read failed: %v", iter, err)
		}
	}
	_, err := res.MetaData()
	if err != nil {
		b.Fatalf("iteration %d metadata failed: %v", iter, err)
	}
}

func BenchmarkIntQuerySimple(b *testing.B) {
	ctx := context.Background()
	q := intBenchQuery(b)

	opts := &cbqueryx.QueryOptions{
		Statement: `SELECT "hello" AS greeting`,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res, err := q.Query(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
		drainRows(b, res, i)
	}
}

func BenchmarkIntQueryScanRows(b *testing.B) {
	ctx := context.Background()
	q := intBenchQuery(b)

	opts := &cbqueryx.QueryOptions{
		Statement: fmt.Sprintf("SELECT * FROM `%s` LIMIT 100", testutilsint.TestOpts.BucketName),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res, err := q.Query(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
		drainRows(b, res, i)
	}
}

func BenchmarkIntQueryPrepared(b *testing.B) {
	ctx := context.Background()
	q := intBenchQuery(b)

	cache := cbqueryx.NewPreparedStatementCache()
	pq := cbqueryx.PreparedQuery{
		Executor: &q,
		Cache:    cache,
	}

	opts := &cbqueryx.QueryOptions{
		Statement: `SELECT "hello" AS greeting`,
	}

	// Warm the cache with one execution.
	res, err := pq.PreparedQuery(ctx, opts)
	if err != nil {
		b.Fatalf("warmup failed: %v", err)
	}
	drainRows(b, res, -1)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res, err := pq.PreparedQuery(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
		drainRows(b, res, i)
	}
}
