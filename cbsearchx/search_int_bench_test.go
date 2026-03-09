package cbsearchx_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/cbsearchx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func intBenchSearchEndpoint(b *testing.B) string {
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
		if nodeExt.Services.Fts > 0 {
			return fmt.Sprintf("http://%s:%d", nodeExt.Hostname, nodeExt.Services.Fts)
		}
	}

	b.Skip("skipping benchmark, no search nodes available")
	return ""
}

func intBenchSearch(b *testing.B) cbsearchx.Search {
	if !testutilsint.TestOpts.LongTest {
		b.Skipf("skipping long benchmark")
	}

	return cbsearchx.Search{
		Logger:    zap.NewNop(),
		Transport: http.DefaultTransport,
		UserAgent: "bench-agent",
		Endpoint:  intBenchSearchEndpoint(b),
		Auth: &cbhttpx.BasicAuth{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
	}
}

func intBenchSearchSetupIndex(b *testing.B, search cbsearchx.Search) string {
	ctx := context.Background()
	indexName := "bench-" + uuid.NewString()[:8]

	_, err := search.UpsertIndex(ctx, &cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutilsint.TestOpts.BucketName,
		},
	})
	require.NoError(b, err)

	b.Cleanup(func() {
		_ = search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
	})

	// Wait for the index to be queryable.
	require.Eventually(b, func() bool {
		_, err := search.Query(ctx, &cbsearchx.QueryOptions{
			IndexName: indexName,
			Query:     &cbsearchx.MatchAllQuery{},
			Size:      1,
		})
		if err != nil {
			if errors.Is(err, cbsearchx.ErrNoIndexPartitionsPlanned) ||
				errors.Is(err, cbsearchx.ErrNoIndexPartitionsFound) ||
				errors.Is(err, cbsearchx.ErrIndexNotFound) ||
				strings.Contains(err.Error(), "no planPIndexes for index") {
				return false
			}
			b.Fatalf("unexpected error while waiting for index to become queryable: %v", err)
		}
		return true
	}, 60*time.Second, 5*time.Second, "index did not become queryable in time")

	return indexName
}

func BenchmarkIntSearch(b *testing.B) {
	search := intBenchSearch(b)
	indexName := intBenchSearchSetupIndex(b, search)
	ctx := context.Background()

	b.Run("Simple", func(b *testing.B) {
		opts := &cbsearchx.QueryOptions{
			IndexName: indexName,
			Query:     &cbsearchx.MatchAllQuery{},
			Size:      1,
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			res, err := search.Query(ctx, opts)
			if err != nil {
				b.Fatalf("iteration %d failed: %v", i, err)
			}
			drainSearchHits(b, res, i)
		}
	})

	b.Run("ManyHits", func(b *testing.B) {
		opts := &cbsearchx.QueryOptions{
			IndexName: indexName,
			Query:     &cbsearchx.MatchAllQuery{},
			Size:      100,
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			res, err := search.Query(ctx, opts)
			if err != nil {
				b.Fatalf("iteration %d failed: %v", i, err)
			}
			drainSearchHits(b, res, i)
		}
	})
}
