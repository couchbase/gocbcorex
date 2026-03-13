package cbsearchx_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
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

// intBenchAgent creates an agent for integration benchmarks
func intBenchAgent(b *testing.B) *gocbcorex.Agent {
	if !testutilsint.TestOpts.LongTest {
		b.Skipf("skipping long benchmark")
	}

	logger := zap.NewNop()

	opts := gocbcorex.AgentOptions{
		Logger:     logger,
		BucketName: testutilsint.TestOpts.BucketName,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: testutilsint.TestOpts.HTTPAddrs,
			MemdAddrs: testutilsint.TestOpts.MemdAddrs,
		},
		CompressionConfig: gocbcorex.CompressionConfig{
			EnableCompression: true,
		},
		DisableMetrics: true,
	}

	agent, err := gocbcorex.CreateAgent(b.Context(), opts)
	require.NoError(b, err)

	b.Cleanup(func() {
		err := agent.Close()
		if err != nil {
			b.Errorf("failed to close agent: %v", err)
		}
	})

	return agent
}

func intBenchSearchSetupIndex(b *testing.B, search cbsearchx.Search) string {
	ctx := context.Background()
	indexName := "benchmark-index"

	agent := intBenchAgent(b)
	opts := &gocbcorex.UpsertOptions{
		Key:            []byte("bench-key-" + uuid.NewString()),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"value":"abcdefg"}`),
	}

	_, err := agent.Upsert(ctx, opts)
	require.NoError(b, err)

	_, err = search.UpsertIndex(ctx, &cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutilsint.TestOpts.BucketName,
		},
	})

	if errors.Is(err, cbsearchx.ErrIndexExists) {
		// If the index already exists, we can just continue.
	} else {
		require.NoError(b, err)
	}

	// Wait for documents to be indexed.
	require.Eventually(b, func() bool {
		count, err := search.GetIndexedDocumentsCount(ctx, &cbsearchx.GetIndexedDocumentsCountOptions{
			IndexName: indexName,
		})

		if err != nil {
			if errors.Is(err, cbsearchx.ErrNoIndexPartitionsPlanned) || errors.Is(err, cbsearchx.ErrNoIndexPartitionsFound) || errors.Is(err, cbsearchx.ErrIndexNotFound) {
				// Transient/expected errors while the index is still coming online.
				return false
			}

			b.Fatalf("unexpected error waiting for index to come online: %v", err)
		}

		return count > 0
	}, 120*time.Second, 10*time.Second, "index did not become queryable in time")

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
			drainSearchHits(b, res)
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
			drainSearchHits(b, res)
		}
	})
}
