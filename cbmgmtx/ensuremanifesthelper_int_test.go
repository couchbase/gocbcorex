package cbmgmtx_test

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEnsureManifestDino(t *testing.T) {
	testutilsint.SkipIfNoDinoCluster(t)

	ctx := context.Background()
	transport := http.DefaultTransport
	bucketName := testutilsint.TestOpts.BucketName
	scopeName := "_default"
	testCollectionName := "testscope-" + uuid.NewString()[:6]

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

	createTestCollection := func() uint64 {
		var manifestUid uint64
		require.Eventually(t, func() bool {
			log.Printf("attempting to create the collection")
			createResp, err := mgmt.CreateCollection(ctx, &cbmgmtx.CreateCollectionOptions{
				BucketName:     bucketName,
				ScopeName:      scopeName,
				CollectionName: testCollectionName,
				MaxTTL:         0,
			})
			if err != nil {
				log.Printf("collection creation failed with error: %s", err)
				return false
			}

			manifestUid, _ = strconv.ParseUint(createResp.ManifestUid, 16, 64)

			return true
		}, 120*time.Second, 1*time.Second)

		return manifestUid
	}

	deleteTestCollection := func() uint64 {
		var manifestUid uint64
		require.Eventually(t, func() bool {
			log.Printf("attempting to delete the collection")
			resp, err := mgmt.DeleteCollection(ctx, &cbmgmtx.DeleteCollectionOptions{
				BucketName:     bucketName,
				ScopeName:      scopeName,
				CollectionName: testCollectionName,
				OnBehalfOf:     nil,
			})
			if err != nil {
				log.Printf("collection deletion failed with error: %s", err)
				return false
			}

			manifestUid, _ = strconv.ParseUint(resp.ManifestUid, 16, 64)

			return true
		}, 120*time.Second, 1*time.Second)

		return manifestUid
	}

	// start dino testing
	dino := testutilsint.StartDinoTesting(t, true)

	// block access to the first endpoint
	dino.BlockNodeTraffic(blockHost)

	manifestUid := createTestCollection()

	hlpr := cbmgmtx.EnsureManifestHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  bucketName,
		ManifestUid: manifestUid,
	}

	require.Never(t, func() bool {
		res, err := hlpr.Poll(ctx, &cbmgmtx.EnsureManifestPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 5*time.Second, 500*time.Millisecond)

	// stop blocking traffic to the node
	dino.AllowTraffic(blockHost)

	require.Eventually(t, func() bool {
		res, err := hlpr.Poll(ctx, &cbmgmtx.EnsureManifestPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 500*time.Millisecond)

	// now lets block traffic again before we delete
	dino.BlockNodeTraffic(blockHost)

	// delete the bucket
	manifestUid = deleteTestCollection()

	hlprOut := cbmgmtx.EnsureManifestHelper{
		Logger:    testutils.MakeTestLogger(t),
		UserAgent: "useragent",

		BucketName:  bucketName,
		ManifestUid: manifestUid,
	}

	require.Never(t, func() bool {
		res, err := hlprOut.Poll(ctx, &cbmgmtx.EnsureManifestPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 5*time.Second, 500*time.Millisecond)

	// stop blocking traffic to the node
	dino.AllowTraffic(blockHost)

	require.Eventually(t, func() bool {
		res, err := hlprOut.Poll(ctx, &cbmgmtx.EnsureManifestPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 500*time.Millisecond)
}
