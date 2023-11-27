package cbmgmtx

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/require"
)

func TestEnsureManifest(t *testing.T) {
	testutils.SkipIfShortTest(t)

	ctx := context.Background()
	bucketName := testutils.TestOpts.BucketName
	scopeName := "_default"
	testCollectionName := "testscope-" + testutils.TestOpts.RunName
	transport := http.DefaultTransport

	config, err := Management{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  "http://" + testutils.TestOpts.HTTPAddrs[0],
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}.GetTerseClusterConfig(ctx, &GetTerseClusterConfigOptions{
		OnBehalfOf: nil,
	})
	require.NoError(t, err)

	var targets []NodeTarget
	for _, nodeExt := range config.NodesExt {
		targets = append(targets, NodeTarget{
			Endpoint: fmt.Sprintf("http://%s:%d", nodeExt.Hostname, nodeExt.Services.Mgmt),
			Username: testutils.TestOpts.Username,
			Password: testutils.TestOpts.Password,
		})
	}

	// we intentionally use the last target that will be polled as the node
	// to create the bucket with so we don't unintentionally give additional
	// time for nodes to sync their configuration
	mgmt := Management{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  targets[len(targets)-1].Endpoint,
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}

	createResp, err := mgmt.CreateCollection(ctx, &CreateCollectionOptions{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: testCollectionName,
		MaxTTL:         0,
	})
	require.NoError(t, err)

	manifestUid, _ := strconv.ParseUint(createResp.ManifestUid, 16, 64)

	t.Cleanup(func() {
		_, err = mgmt.DeleteCollection(ctx, &DeleteCollectionOptions{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: testCollectionName,
			OnBehalfOf:     nil,
		})
		require.NoError(t, err)
	})

	hlpr := EnsureManifestHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  "default",
		ManifestUid: manifestUid,
	}

	require.Eventually(t, func() bool {
		res, err := hlpr.Poll(ctx, &EnsureManifestPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)
}
