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
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEnsureManifestDino(t *testing.T) {
	testutils.SkipIfNoDinoCluster(t)

	ctx := context.Background()
	bucketName := testutils.TestOpts.BucketName
	scopeName := "_default"
	testCollectionName := "testscope-" + uuid.NewString()[:6]
	transport := http.DefaultTransport

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

	// we set up a cleanup function to ensure that traffic is allowed so we
	// don't accidentally leave the cluster in a randomly unusable state
	// if an error occurs during the test.
	t.Cleanup(func() {
		testutils.DinoAllowTraffic(t, blockHost)
	})

	// block access to the first endpoint
	testutils.DinoBlockTraffic(t, blockHost)

	manifestUid := createTestCollection()

	hlpr := cbmgmtx.EnsureManifestHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  "default",
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
	testutils.DinoAllowTraffic(t, blockHost)

	require.Eventually(t, func() bool {
		res, err := hlpr.Poll(ctx, &cbmgmtx.EnsureManifestPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 500*time.Millisecond)

	// now lets block traffic again before we delete
	testutils.DinoBlockTraffic(t, blockHost)

	// delete the bucket
	manifestUid = deleteTestCollection()

	hlprOut := cbmgmtx.EnsureManifestHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  "default",
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
	testutils.DinoAllowTraffic(t, blockHost)

	require.Eventually(t, func() bool {
		res, err := hlprOut.Poll(ctx, &cbmgmtx.EnsureManifestPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 500*time.Millisecond)
}
