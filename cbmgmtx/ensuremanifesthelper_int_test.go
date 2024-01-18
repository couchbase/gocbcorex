package cbmgmtx_test

import (
	"context"
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

func TestEnsureManifest(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()
	bucketName := testutilsint.TestOpts.BucketName
	scopeName := "_default"
	testCollectionName := "testscope-" + uuid.NewString()[:6]
	transport := http.DefaultTransport

	nodes := testutilsint.GetTestNodes(t)

	var targets []cbmgmtx.NodeTarget
	for _, node := range nodes {
		targets = append(targets, cbmgmtx.NodeTarget{
			Endpoint: node.NsEndpoint(),
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		})
	}

	// we intentionally use the last target that will be polled as the node
	// to create the bucket with so we don't unintentionally give additional
	// time for nodes to sync their configuration
	mgmt := cbmgmtx.Management{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  targets[len(targets)-1].Endpoint,
		Username:  testutilsint.TestOpts.Username,
		Password:  testutilsint.TestOpts.Password,
	}

	createResp, err := mgmt.CreateCollection(ctx, &cbmgmtx.CreateCollectionOptions{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: testCollectionName,
		MaxTTL:         0,
	})
	require.NoError(t, err)

	manifestUid, _ := strconv.ParseUint(createResp.ManifestUid, 16, 64)

	t.Cleanup(func() {
		_, err = mgmt.DeleteCollection(ctx, &cbmgmtx.DeleteCollectionOptions{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: testCollectionName,
			OnBehalfOf:     nil,
		})
		require.NoError(t, err)
	})

	hlpr := cbmgmtx.EnsureManifestHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  "default",
		ManifestUid: manifestUid,
	}

	require.Eventually(t, func() bool {
		res, err := hlpr.Poll(ctx, &cbmgmtx.EnsureManifestPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 500*time.Millisecond)
}
