package cbsearchx

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestIndexPolling(t *testing.T) {
	testutils.SkipIfShortTest(t)

	ctx := context.Background()
	transport := http.DefaultTransport

	config, err := cbmgmtx.Management{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  "http://" + testutils.TestOpts.HTTPAddrs[0],
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}.GetTerseClusterConfig(ctx, &cbmgmtx.GetTerseClusterConfigOptions{
		OnBehalfOf: nil,
	})
	require.NoError(t, err)

	var targets []NodeTarget
	for _, nodeExt := range config.NodesExt {
		if nodeExt.Services.Fts > 0 {
			targets = append(targets, NodeTarget{
				Endpoint: fmt.Sprintf("http://%s:%d", nodeExt.Hostname, nodeExt.Services.Fts),
				Username: testutils.TestOpts.Username,
				Password: testutils.TestOpts.Password,
			})
		}
	}
	if len(targets) == 0 {
		t.Skip("Skipping test, no search nodes")
	}

	// we intentionally use the last target that will be polled as the node
	// to create the index with so we don't unintentionally give additional
	// time for nodes to sync their configuration
	search := Search{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  targets[len(targets)-1].Endpoint,
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}

	indexName := "a" + uuid.NewString()[:6]
	err = search.UpsertIndex(ctx, &UpsertIndexOptions{
		Index: Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutils.TestOpts.BucketName,
		},
	})
	require.NoError(t, err)

	hlpr := EnsureIndexHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		IndexName: indexName,
	}

	require.Eventually(t, func() bool {
		res, err := hlpr.PollCreated(ctx, &EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)

	err = search.DeleteIndex(ctx, &DeleteIndexOptions{
		IndexName: indexName,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		res, err := hlpr.PollDropped(ctx, &EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)
}
