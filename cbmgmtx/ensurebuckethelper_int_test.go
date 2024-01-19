package cbmgmtx_test

import (
	"context"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEnsureBucketDino(t *testing.T) {
	testutilsint.SkipIfNoDinoCluster(t)

	ctx := context.Background()
	transport := http.DefaultTransport
	testBucketName := "testbucket-" + uuid.NewString()[:6]

	nodes := testutilsint.GetTestNodes(t)

	blockNode := nodes.SelectFirst(t, func(node *testutilsint.NodeTarget) bool {
		return !node.IsOrchestrator
	})
	execNode := nodes.SelectLast(t, func(node *testutilsint.NodeTarget) bool {
		return node != blockNode
	})

	blockHost := blockNode.Hostname
	execEndpoint := execNode.NsEndpoint()

	log.Printf("nodes:")
	for _, node := range nodes {
		log.Printf("  %s", node)
	}
	log.Printf("execution endpoint: %s", execEndpoint)
	log.Printf("blocked host: %s", blockHost)

	var targets []cbmgmtx.NodeTarget
	for _, node := range nodes {
		targets = append(targets, cbmgmtx.NodeTarget{
			Endpoint: node.NsEndpoint(),
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		})
	}

	mgmt := cbmgmtx.Management{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  execEndpoint,
		Username:  testutilsint.TestOpts.Username,
		Password:  testutilsint.TestOpts.Password,
	}

	createTestBucket := func() {
		require.Eventually(t, func() bool {
			log.Printf("attempting to create the bucket")
			err := mgmt.CreateBucket(ctx, &cbmgmtx.CreateBucketOptions{
				BucketName: testBucketName,
				BucketSettings: cbmgmtx.BucketSettings{
					MutableBucketSettings: cbmgmtx.MutableBucketSettings{
						RAMQuotaMB:         100,
						EvictionPolicy:     cbmgmtx.EvictionPolicyTypeValueOnly,
						CompressionMode:    cbmgmtx.CompressionModePassive,
						DurabilityMinLevel: cbmgmtx.DurabilityLevelNone,
					},
					ConflictResolutionType: cbmgmtx.ConflictResolutionTypeSequenceNumber,
					BucketType:             cbmgmtx.BucketTypeCouchbase,
					StorageBackend:         cbmgmtx.StorageBackendCouchstore,
					ReplicaIndex:           true,
				},
			})
			if err != nil {
				log.Printf("bucket creation failed with error: %s", err)
				return false
			}

			return true
		}, 120*time.Second, 1*time.Second)
	}

	deleteTestBucket := func() {
		require.Eventually(t, func() bool {
			log.Printf("attempting to delete the bucket")
			err := mgmt.DeleteBucket(ctx, &cbmgmtx.DeleteBucketOptions{
				BucketName: testBucketName,
			})
			if err != nil {
				log.Printf("bucket deletion failed with error: %s", err)
				return false
			}

			return true
		}, 120*time.Second, 1*time.Second)
	}

	// start dino testing
	dino := testutilsint.StartDinoTesting(t, true)

	// block access to the first endpoint
	dino.BlockTraffic(blockHost)

	// create the test bucket
	createTestBucket()

	hlpr := cbmgmtx.EnsureBucketHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  testBucketName,
		BucketUUID:  "",
		WantMissing: false,
	}

	// the first couple of polls should fail, since a node is unavailable
	require.Never(t, func() bool {
		res, err := hlpr.Poll(ctx, &cbmgmtx.EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 5*time.Second, 500*time.Millisecond)

	// stop blocking traffic to the node
	dino.AllowTraffic(blockHost)

	// we should see that the polls eventually succeed
	require.Eventually(t, func() bool {
		res, err := hlpr.Poll(ctx, &cbmgmtx.EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)

	// now lets block traffic again before we delete
	dino.BlockTraffic(blockHost)

	// delete the bucket
	deleteTestBucket()

	hlprDel := cbmgmtx.EnsureBucketHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  testBucketName,
		BucketUUID:  "",
		WantMissing: true,
	}

	// the first couple of polls should fail, since a node is unavailable
	require.Never(t, func() bool {
		res, err := hlprDel.Poll(ctx, &cbmgmtx.EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 5*time.Second, 500*time.Millisecond)

	// stop blocking traffic to the node
	dino.AllowTraffic(blockHost)

	// we should see that the polls eventually succeed
	require.Eventually(t, func() bool {
		res, err := hlprDel.Poll(ctx, &cbmgmtx.EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 500*time.Millisecond)
}
