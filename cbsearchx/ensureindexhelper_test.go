package cbsearchx_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbsearchx"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testNodesToNodeTargets(t *testing.T, nodes []testutils.TestNodeTarget) []cbsearchx.NodeTarget {
	var targets []cbsearchx.NodeTarget
	for _, node := range nodes {
		if node.SearchPort == 0 {
			continue
		}
		targets = append(targets, cbsearchx.NodeTarget{
			Endpoint: fmt.Sprintf("http://%s:%d", node.Hostname, node.SearchPort),
			Username: testutils.TestOpts.Username,
			Password: testutils.TestOpts.Password,
		})
	}
	return targets
}

func TestEnsureIndex(t *testing.T) {
	testutils.SkipIfShortTest(t)

	ctx := context.Background()
	transport := http.DefaultTransport

	blockHost, execEndpoint, baseTargets := testutils.SelectNodeToBlock(t, testutils.TestServiceSearch)
	targets := testNodesToNodeTargets(t, baseTargets)

	testutils.DisableAutoFailover(t)

	// we intentionally use the last target that will be polled as the node
	// to create the index with so we don't unintentionally give additional
	// time for nodes to sync their configuration
	search := cbsearchx.Search{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  execEndpoint,
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}

	indexName := "a" + uuid.NewString()[:6]

	upsertTestIndex := func() {
		require.Eventually(t, func() bool {
			log.Printf("attempting to create the index")
			err := search.UpsertIndex(ctx, &cbsearchx.UpsertIndexOptions{
				Index: cbsearchx.Index{
					Name:       indexName,
					Type:       "fulltext-index",
					SourceType: "couchbase",
					SourceName: testutils.TestOpts.BucketName,
				},
			})
			if err != nil {
				log.Printf("index creation failed with error: %s", err)
				return false
			}

			return true
		}, 120*time.Second, 1*time.Second)
	}

	deleteTestIndex := func() {
		require.Eventually(t, func() bool {
			log.Printf("attempting to delete the index")
			err := search.DeleteIndex(ctx, &cbsearchx.DeleteIndexOptions{
				IndexName: indexName,
			})
			if err != nil {
				log.Printf("index deletion failed with error: %s", err)
				return false
			}

			return true
		}, 120*time.Second, 1*time.Second)
	}
	// we set up a cleanup function to ensure that traffic is allowed so we
	// don't accidentally leave the cluster in a randomly unusable state
	// if an error occurs during the test.
	t.Cleanup(func() {
		testutils.DinoAllowTraffic(t, blockHost)
	})

	// block access to the first endpoint
	testutils.DinoBlockTraffic(t, blockHost)

	upsertTestIndex()

	hlpr := cbsearchx.EnsureIndexHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		IndexName: indexName,
	}

	require.Never(t, func() bool {
		res, err := hlpr.PollCreated(ctx, &cbsearchx.EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		if err != nil {
			log.Printf("index creation failed with error: %s", err)
			return false
		}

		return res
	}, 5*time.Second, 1*time.Second)

	// stop blocking traffic to the node
	testutils.DinoAllowTraffic(t, blockHost)

	require.Eventually(t, func() bool {
		res, err := hlpr.PollCreated(ctx, &cbsearchx.EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		if err != nil {
			log.Printf("index creation failed with error: %s", err)
			return false
		}

		return res
	}, 30*time.Second, 1*time.Second)

	// now lets block traffic again before we delete
	testutils.DinoBlockTraffic(t, blockHost)

	hlprOut := cbsearchx.EnsureIndexHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		IndexName: indexName,
	}

	deleteTestIndex()

	require.Never(t, func() bool {
		res, err := hlprOut.PollDropped(ctx, &cbsearchx.EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		if err != nil {
			log.Printf("index deletion failed with error: %s", err)
			return false
		}

		return res
	}, 5*time.Second, 1*time.Second)

	// stop blocking traffic to the node
	testutils.DinoAllowTraffic(t, blockHost)

	require.Eventually(t, func() bool {
		res, err := hlprOut.PollDropped(ctx, &cbsearchx.EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		if err != nil {
			log.Printf("index deletion failed with error: %s", err)
			return false
		}

		return res
	}, 30*time.Second, 1*time.Second)
}
