package cbsearchx_test

import (
	"context"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbsearchx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEnsureIndexDino(t *testing.T) {
	testutilsint.SkipIfNoDinoCluster(t)

	ctx := context.Background()
	transport := http.DefaultTransport

	nodes := testutilsint.GetTestNodes(t)

	blockNode := nodes.SelectFirst(t, func(node *testutilsint.NodeTarget) bool {
		return !node.IsOrchestrator && node.SearchPort != 0
	})
	execNode := nodes.SelectLast(t, func(node *testutilsint.NodeTarget) bool {
		return node != blockNode && node.SearchPort != 0
	})

	blockHost := blockNode.Hostname
	execEndpoint := execNode.SearchEndpoint()

	log.Printf("nodes:")
	for _, node := range nodes {
		log.Printf("  %s", node)
	}
	log.Printf("execution endpoint: %s", execEndpoint)
	log.Printf("blocked host: %s", blockHost)

	var targets []cbsearchx.NodeTarget
	for _, searchNode := range nodes {
		targets = append(targets, cbsearchx.NodeTarget{
			Endpoint: searchNode.SearchEndpoint(),
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		})
	}

	search := cbsearchx.Search{
		Logger:    testutils.MakeTestLogger(t),
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  execEndpoint,
		Username:  testutilsint.TestOpts.Username,
		Password:  testutilsint.TestOpts.Password,
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
					SourceName: testutilsint.TestOpts.BucketName,
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

	// start dino testing
	dino := testutilsint.StartDinoTesting(t, true)

	// block access to the first endpoint
	dino.BlockNodeTraffic(blockHost)

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
	dino.AllowTraffic(blockHost)

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
	dino.BlockNodeTraffic(blockHost)

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
	dino.AllowTraffic(blockHost)

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
