package cbmgmtx_test

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
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
		Auth: &cbhttpx.BasicAuth{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
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

	modifyTestBucket := func() {
		require.Eventually(t, func() bool {
			log.Printf("attempting to modify the bucket")
			err := mgmt.UpdateBucket(ctx, &cbmgmtx.UpdateBucketOptions{
				BucketName: testBucketName,
				MutableBucketSettings: cbmgmtx.MutableBucketSettings{
					RAMQuotaMB: 110,
				},
			})
			if err != nil {
				log.Printf("bucket modification failed with error: %s", err)
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
				// Workaround for MB-64034
				if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
					return true
				}

				log.Printf("bucket deletion failed with error: %s", err)
				return false
			}

			return true
		}, 120*time.Second, 1*time.Second)
	}

	// start dino testing
	dino := testutilsint.StartDinoTesting(t, true)

	// block access to the first endpoint
	dino.BlockNodeTraffic(blockHost)

	// create the test bucket
	createTestBucket()

	var syncCreate sync.Mutex
	hlprCreate := cbmgmtx.EnsureBucketHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  testBucketName,
		BucketUUID:  "",
		WantHealthy: true,
		WantMissing: false,
	}

	// the first couple of polls should fail, since a node is unavailable
	require.Never(t, func() bool {
		syncCreate.Lock()
		defer syncCreate.Unlock()

		res, err := hlprCreate.Poll(ctx, &cbmgmtx.EnsureBucketPollOptions{
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
		syncCreate.Lock()
		defer syncCreate.Unlock()

		res, err := hlprCreate.Poll(ctx, &cbmgmtx.EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 90*time.Second, 1*time.Second)

	// now lets block traffic again before we modify
	dino.BlockNodeTraffic(blockHost)

	// modify the bucket
	modifyTestBucket()

	var syncMod sync.Mutex
	hlprMod := cbmgmtx.EnsureBucketHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  testBucketName,
		BucketUUID:  "",
		WantMissing: false,
		WantSettings: &cbmgmtx.MutableBucketSettings{
			RAMQuotaMB: 110,
		},
	}

	// the first couple of polls should fail, since a node is unavailable
	require.Never(t, func() bool {
		syncMod.Lock()
		defer syncMod.Unlock()

		res, err := hlprMod.Poll(ctx, &cbmgmtx.EnsureBucketPollOptions{
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
		syncMod.Lock()
		defer syncMod.Unlock()

		res, err := hlprMod.Poll(ctx, &cbmgmtx.EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)

	// now lets block traffic again before we delete
	dino.BlockNodeTraffic(blockHost)

	// delete the bucket
	deleteTestBucket()

	var syncDel sync.Mutex
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
		syncDel.Lock()
		defer syncDel.Unlock()

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
		syncDel.Lock()
		defer syncDel.Unlock()

		res, err := hlprDel.Poll(ctx, &cbmgmtx.EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 500*time.Millisecond)
}
