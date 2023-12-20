package cbqueryx_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbqueryx"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testNodesToNodeTargets(t *testing.T, nodes []testutils.TestNodeTarget) []cbqueryx.NodeTarget {
	var targets []cbqueryx.NodeTarget
	for _, node := range nodes {
		if node.QueryPort == 0 {
			continue
		}
		targets = append(targets, cbqueryx.NodeTarget{
			Endpoint: fmt.Sprintf("http://%s:%d", node.Hostname, node.QueryPort),
			Username: testutils.TestOpts.Username,
			Password: testutils.TestOpts.Password,
		})
	}
	return targets
}

func TestEnsureQueryDino(t *testing.T) {
	testutils.SkipIfNoDinoCluster(t)

	ctx := context.Background()
	transport := http.DefaultTransport

	blockHost, execEndpoint, baseTargets := testutils.SelectNodeToBlock(t, testutils.TestServiceQuery)
	targets := testNodesToNodeTargets(t, baseTargets)

	testutils.DisableAutoFailover(t)

	if blockHost == "" {
		// This test requires a very specific cluster setup containing a node with the query service but not indexing.
		// The test blocks traffic to that query node.
		t.Skipf("Cluster not in configuration that can be used by this test")
	}

	query := cbqueryx.Query{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  execEndpoint,
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
		Logger:    testutils.MakeTestLogger(t),
	}

	idxName := uuid.NewString()[:6]

	createTestIndex := func() {
		require.Eventually(t, func() bool {
			log.Printf("attempting to create the index")
			res, err := query.Query(ctx, &cbqueryx.Options{
				Statement: fmt.Sprintf(
					"CREATE INDEX `%s` On `%s`._default._default(test)",
					idxName,
					testutils.TestOpts.BucketName,
				),
			})
			if err != nil {
				log.Printf("index creation failed with error: %s", err)
				return false
			}

			for res.HasMoreRows() {
			}

			return true
		}, 120*time.Second, 1*time.Second)

	}

	deleteTestIndex := func() {
		require.Eventually(t, func() bool {
			log.Printf("attempting to delete the index")
			res, err := query.Query(ctx, &cbqueryx.Options{
				Statement: fmt.Sprintf(
					"DROP INDEX `%s` ON `%s`.`_default`.`_default`",
					idxName,
					testutils.TestOpts.BucketName,
				),
			})
			if err != nil {
				log.Printf("index deletion failed with error: %s", err)
				return false
			}

			for res.HasMoreRows() {
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

	createTestIndex()

	hlpr := cbqueryx.EnsureIndexHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:     testutils.TestOpts.BucketName,
		ScopeName:      "_default",
		CollectionName: "_default",
		IndexName:      idxName,
	}

	require.Never(t, func() bool {
		res, err := hlpr.PollCreated(ctx, &cbqueryx.EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 5*time.Second, 1*time.Second)

	// stop blocking traffic to the node
	testutils.DinoAllowTraffic(t, blockHost)

	require.Eventually(t, func() bool {
		res, err := hlpr.PollCreated(ctx, &cbqueryx.EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)

	// now lets block traffic again before we delete
	testutils.DinoBlockTraffic(t, blockHost)

	hlprOut := cbqueryx.EnsureIndexHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:     testutils.TestOpts.BucketName,
		ScopeName:      "_default",
		CollectionName: "_default",
		IndexName:      idxName,
	}

	deleteTestIndex()

	require.Never(t, func() bool {
		res, err := hlprOut.PollDropped(ctx, &cbqueryx.EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 5*time.Second, 1*time.Second)

	// stop blocking traffic to the node
	testutils.DinoAllowTraffic(t, blockHost)

	require.Eventually(t, func() bool {
		res, err := hlprOut.PollDropped(ctx, &cbqueryx.EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)
}
