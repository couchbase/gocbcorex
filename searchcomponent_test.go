package gocbcorex

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/cbsearchx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func newSearchIndexName() string {
	indexName := "a" + uuid.New().String()
	return indexName
}

func getSearchEndpoints(t *testing.T) []string {
	config, err := cbmgmtx.Management{
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  "http://" + testutils.TestOpts.HTTPAddrs[0],
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}.GetTerseClusterConfig(context.Background(), &cbmgmtx.GetTerseClusterConfigOptions{
		OnBehalfOf: nil,
	})
	require.NoError(t, err)

	var endpoints []string
	for _, nodeExt := range config.NodesExt {
		if nodeExt.Services.Fts > 0 {
			endpoints = append(endpoints, fmt.Sprintf("http://%s:%d", nodeExt.Hostname, nodeExt.Services.Fts))
		}
	}

	return endpoints
}

func TestEnsureIndex(t *testing.T) {
	testutils.SkipIfShortTest(t)
	retries := NewRetryManagerDefault()
	endpoints := getSearchEndpoints(t)
	sC := NewSearchComponent(
		retries,
		&SearchComponentConfig{
			HttpRoundTripper: http.DefaultTransport,
			Endpoints:        endpoints,
			Authenticator: &PasswordAuthenticator{
				Username: testutils.TestOpts.Username,
				Password: testutils.TestOpts.Password,
			},
		},
		&SearchComponentOptions{
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
			SourceName: testutils.TestOpts.BucketName,
		},
	})
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		err := sC.EnsureIndexCreated(ctx, &EnsureSearchIndexCreatedOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		err = sC.DeleteIndex(ctx, &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		err = sC.EnsureIndexDropped(ctx, &EnsureSearchIndexDroppedOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	t.Run("ScopedSuccess", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		scopedName := newSearchIndexName()
		err := sC.UpsertIndex(ctx, &cbsearchx.UpsertIndexOptions{
			BucketName: "someBucket",
			ScopeName:  "someScope",
			Index: cbsearchx.Index{
				Name:       scopedName,
				Type:       "fulltext-index",
				SourceType: "couchbase",
				SourceName: testutils.TestOpts.BucketName,
			},
		})
		require.NoError(t, err)

		err = sC.EnsureIndexCreated(ctx, &EnsureSearchIndexCreatedOptions{
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

		err = sC.EnsureIndexDropped(ctx, &EnsureSearchIndexDroppedOptions{
			IndexName:  scopedName,
			BucketName: "someBucket",
			ScopeName:  "someScope",
		})
		require.NoError(t, err)
	})
}
