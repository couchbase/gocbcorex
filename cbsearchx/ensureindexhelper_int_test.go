package cbsearchx_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbsearchx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestIndexPolling(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()
	transport := http.DefaultTransport

	nodes := testutilsint.GetTestNodes(t)
	searchNodes := nodes.Select(func(node *testutilsint.NodeTarget) bool {
		return node.SearchPort > 0
	})
	if len(searchNodes) == 0 {
		t.Skip("Skipping test, no search nodes")
	}

	var targets []cbsearchx.NodeTarget
	for _, queryNode := range searchNodes {
		targets = append(targets, cbsearchx.NodeTarget{
			Endpoint: queryNode.SearchEndpoint(),
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		})
	}

	// we intentionally use the last target that will be polled as the node
	// to create the index with so we don't unintentionally give additional
	// time for nodes to sync their configuration
	search := cbsearchx.Search{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  targets[len(targets)-1].Endpoint,
		Username:  testutilsint.TestOpts.Username,
		Password:  testutilsint.TestOpts.Password,
	}

	indexName := "a" + uuid.NewString()[:6]
	err := search.UpsertIndex(ctx, &cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutilsint.TestOpts.BucketName,
		},
	})
	require.NoError(t, err)

	hlpr := cbsearchx.EnsureIndexHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		IndexName: indexName,
	}

	require.Eventually(t, func() bool {
		res, err := hlpr.PollCreated(ctx, &cbsearchx.EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)

	err = search.DeleteIndex(ctx, &cbsearchx.DeleteIndexOptions{
		IndexName: indexName,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		res, err := hlpr.PollDropped(ctx, &cbsearchx.EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)
}
