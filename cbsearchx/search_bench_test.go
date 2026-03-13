package cbsearchx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbsearchx"
	"go.uber.org/zap"
)

func benchSearchResultBody(numHits int) []byte {
	hits := make([]json.RawMessage, numHits)
	for i := range hits {
		hits[i] = json.RawMessage(fmt.Sprintf(
			`{"index":"idx_1","id":"doc-%d","score":1.5,"explanation":null,"locations":{},"fragments":{},"fields":{}}`,
			i,
		))
	}

	hitsBytes, _ := json.Marshal(hits)

	return []byte(fmt.Sprintf(
		`{"status":{"total":1,"failed":0,"successful":1},"total_hits":%d,"max_score":1.5,"took":1000000,"hits":%s,"facets":{}}`,
		numHits,
		string(hitsBytes),
	))
}

type searchBenchRoundTripper struct {
	body []byte
}

func (rt *searchBenchRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode:    200,
		Body:          io.NopCloser(bytes.NewReader(rt.body)),
		ContentLength: int64(len(rt.body)),
	}, nil
}

func newBenchSearch(rt http.RoundTripper) cbsearchx.Search {
	return cbsearchx.Search{
		Logger:    zap.NewNop(),
		Transport: rt,
		UserAgent: "bench-agent",
		Endpoint:  "http://localhost:8094",
		Auth: &cbhttpx.BasicAuth{
			Username: "user",
			Password: "pass",
		},
	}
}

func drainSearchHits(b *testing.B, res cbsearchx.QueryResultStream) {
	for res.HasMoreHits() {
		_, err := res.ReadHit()
		if err != nil {
			b.Fatalf("hit read failed: %v", err)
		}
	}
	_, err := res.MetaData()
	if err != nil {
		b.Fatalf("metadata read failed: %v", err)
	}
}

func BenchmarkSearchSimple(b *testing.B) {
	ctx := context.Background()
	body := benchSearchResultBody(2)
	s := newBenchSearch(&searchBenchRoundTripper{body: body})

	opts := &cbsearchx.QueryOptions{
		IndexName: "bench-index",
		Query:     &cbsearchx.MatchAllQuery{},
		Size:      2,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res, err := s.Query(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d query failed: %v", i, err)
		}
		drainSearchHits(b, res)
	}
}

func BenchmarkSearchManyHits(b *testing.B) {
	ctx := context.Background()
	body := benchSearchResultBody(100)
	s := newBenchSearch(&searchBenchRoundTripper{body: body})

	opts := &cbsearchx.QueryOptions{
		IndexName: "bench-index",
		Query:     &cbsearchx.MatchAllQuery{},
		Size:      100,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res, err := s.Query(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d query failed: %v", i, err)
		}
		drainSearchHits(b, res)
	}
}
