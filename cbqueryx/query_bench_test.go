package cbqueryx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"go.uber.org/zap"
)

func benchQueryResultBody(rows []string, prepared string) []byte {
	results := "[]"
	if len(rows) > 0 {
		results = "[" + strings.Join(rows, ",") + "]"
	}

	m := map[string]interface{}{
		"requestID":       "bench-req-id",
		"clientContextID": "bench-ctx-id",
		"status":          "success",
		"metrics": map[string]interface{}{
			"elapsedTime":   "1ms",
			"executionTime": "900us",
			"resultCount":   len(rows),
			"resultSize":    100,
			"serviceLoad":   1,
		},
		"results": json.RawMessage(results),
	}
	if prepared != "" {
		m["prepared"] = prepared
	}

	body, _ := json.Marshal(m)
	return body
}

type benchRoundTripper struct {
	body []byte
}

func (rt *benchRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode:    200,
		Body:          io.NopCloser(bytes.NewReader(rt.body)),
		ContentLength: int64(len(rt.body)),
	}, nil
}

func newBenchQuery(rt http.RoundTripper) cbqueryx.Query {
	return cbqueryx.Query{
		Logger:    zap.NewNop(),
		Transport: rt,
		UserAgent: "bench-agent",
		Endpoint:  "http://localhost:8093",
		Auth: &cbhttpx.BasicAuth{
			Username: "user",
			Password: "pass",
		},
	}
}

func BenchmarkQuerySimple(b *testing.B) {
	ctx := context.Background()
	rows := []string{`{"test":"value"}`, `{"test2":"value2"}`}
	body := benchQueryResultBody(rows, "")
	q := newBenchQuery(&benchRoundTripper{body: body})

	opts := &cbqueryx.QueryOptions{
		Statement: "SELECT 1",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res, err := q.Query(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d query failed: %v", i, err)
		}
		drainRows(b, res, i)
	}
}

func BenchmarkQueryManyRows(b *testing.B) {
	ctx := context.Background()

	rows := make([]string, 100)
	for i := range rows {
		rows[i] = fmt.Sprintf(`{"i":%d,"name":"item-%d","value":"data"}`, i, i)
	}
	body := benchQueryResultBody(rows, "")
	q := newBenchQuery(&benchRoundTripper{body: body})

	opts := &cbqueryx.QueryOptions{
		Statement: "SELECT * FROM bucket LIMIT 100",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res, err := q.Query(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d query failed: %v", i, err)
		}
		drainRows(b, res, i)
	}
}

func BenchmarkQueryPreparedCacheHit(b *testing.B) {
	ctx := context.Background()
	rows := []string{`{"test":"value"}`, `{"test2":"value2"}`}
	body := benchQueryResultBody(rows, "")
	q := newBenchQuery(&benchRoundTripper{body: body})

	cache := cbqueryx.NewPreparedStatementCache()
	cache.Put("SELECT 1", "prepared-name-123")

	opts := &cbqueryx.QueryOptions{
		Statement: "SELECT 1",
	}

	pq := cbqueryx.PreparedQuery{
		Executor: &q,
		Cache:    cache,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res, err := pq.PreparedQuery(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d query failed: %v", i, err)
		}
		drainRows(b, res, i)
	}
}

func BenchmarkQueryPreparedCacheMiss(b *testing.B) {
	ctx := context.Background()
	rows := []string{`{"test":"value"}`, `{"test2":"value2"}`}
	body := benchQueryResultBody(rows, "prepared-name-123")
	q := newBenchQuery(&benchRoundTripper{body: body})

	cache := cbqueryx.NewPreparedStatementCache()

	pq := cbqueryx.PreparedQuery{
		Executor: &q,
		Cache:    cache,
	}

	opts := make([]cbqueryx.QueryOptions, b.N)
	for i := range opts {
		opts[i] = cbqueryx.QueryOptions{
			Statement: fmt.Sprintf("SELECT %d", i),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res, err := pq.PreparedQuery(ctx, &opts[i])
		if err != nil {
			b.Fatalf("iteration %d query failed: %v", i, err)
		}
		drainRows(b, res, i)
	}
}
