package gocbcorex_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/cbsearchx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func newSearchIndexName() string {
	indexName := "a" + uuid.New().String()
	return indexName
}

func getSearchEndpoints(t *testing.T) map[string]string {
	config, err := cbmgmtx.Management{
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  "http://" + testutilsint.TestOpts.HTTPAddrs[0],
		Auth: &cbhttpx.BasicAuth{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
	}.GetTerseClusterConfig(context.Background(), &cbmgmtx.GetTerseClusterConfigOptions{
		OnBehalfOf: nil,
	})
	require.NoError(t, err)

	endpoints := make(map[string]string)
	for nodeIdx, nodeExt := range config.NodesExt {
		if nodeExt.Services.Fts > 0 {
			epId := fmt.Sprintf("ep-%d", nodeIdx)
			endpoints[epId] = fmt.Sprintf("http://%s:%d", nodeExt.Hostname, nodeExt.Services.Fts)
		}
	}

	return endpoints
}

func TestEnsureIndex(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	retries := gocbcorex.NewRetryManagerDefault()
	endpoints := getSearchEndpoints(t)
	sC := gocbcorex.NewSearchComponent(
		retries,
		&gocbcorex.SearchComponentConfig{
			HttpRoundTripper: http.DefaultTransport,
			Endpoints:        endpoints,
			Authenticator: &gocbcorex.PasswordAuthenticator{
				Username: testutilsint.TestOpts.Username,
				Password: testutilsint.TestOpts.Password,
			},
		},
		&gocbcorex.SearchComponentOptions{
			Logger:    testutils.MakeTestLogger(t),
			UserAgent: "useragent",
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	indexName := newSearchIndexName()
	err := sC.UpsertIndex(ctx, &cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutilsint.TestOpts.BucketName,
		},
	})
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		err := sC.EnsureIndexCreated(ctx, &gocbcorex.EnsureSearchIndexCreatedOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		err = sC.DeleteIndex(ctx, &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		err = sC.EnsureIndexDropped(ctx, &gocbcorex.EnsureSearchIndexDroppedOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	t.Run("ScopedSuccess", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		scopedName := newSearchIndexName()
		err := sC.UpsertIndex(ctx, &cbsearchx.UpsertIndexOptions{
			BucketName: "someBucket",
			ScopeName:  "someScope",
			Index: cbsearchx.Index{
				Name:       scopedName,
				Type:       "fulltext-index",
				SourceType: "couchbase",
				SourceName: testutilsint.TestOpts.BucketName,
			},
		})
		require.NoError(t, err)

		err = sC.EnsureIndexCreated(ctx, &gocbcorex.EnsureSearchIndexCreatedOptions{
			IndexName:  scopedName,
			BucketName: "someBucket",
			ScopeName:  "someScope",
		})
		require.NoError(t, err)

		err = sC.DeleteIndex(ctx, &cbsearchx.DeleteIndexOptions{
			BucketName: "someBucket",
			ScopeName:  "someScope",
			IndexName:  scopedName,
		})
		require.NoError(t, err)

		err = sC.EnsureIndexDropped(ctx, &gocbcorex.EnsureSearchIndexDroppedOptions{
			IndexName:  scopedName,
			BucketName: "someBucket",
			ScopeName:  "someScope",
		})
		require.NoError(t, err)
	})
}
