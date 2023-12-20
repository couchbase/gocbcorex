package cbmgmtx_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testNodesToNodeTargets(t *testing.T, nodes []testutils.TestNodeTarget) []cbmgmtx.NodeTarget {
	var targets []cbmgmtx.NodeTarget
	for _, node := range nodes {
		targets = append(targets, cbmgmtx.NodeTarget{
			Endpoint: fmt.Sprintf("http://%s:%d", node.Hostname, node.NsPort),
			Username: testutils.TestOpts.Username,
			Password: testutils.TestOpts.Password,
		})
	}
	return targets
}

func TestEnsureBucketDino(t *testing.T) {
	testutils.SkipIfNoDinoCluster(t)

	ctx := context.Background()
	transport := http.DefaultTransport
	testBucketName := "testbucket-" + uuid.NewString()[:6]

	blockHost, execEndpoint, baseTargets := testutils.SelectNodeToBlock(t, testutils.TestServiceNs)
	targets := testNodesToNodeTargets(t, baseTargets)

	testutils.DisableAutoFailover(t)

	mgmt := cbmgmtx.Management{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  execEndpoint,
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
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

	// we set up a cleanup function to ensure that traffic is allowed so we
	// don't accidentally leave the cluster in a randomly unusable state
	// if an error occurs during the test.
	t.Cleanup(func() {
		testutils.DinoAllowTraffic(t, blockHost)
	})

	// block access to the first endpoint
	testutils.DinoBlockTraffic(t, blockHost)

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
	testutils.DinoAllowTraffic(t, blockHost)

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
	testutils.DinoBlockTraffic(t, blockHost)

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
	testutils.DinoAllowTraffic(t, blockHost)

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
