package cbmgmtx

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEnsureBucket(t *testing.T) {
	testutils.SkipIfShortTest(t)

	ctx := context.Background()
	transport := http.DefaultTransport
	testBucketName := "testbucket-" + uuid.NewString()[:6]

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

	err = mgmt.CreateBucket(ctx, &CreateBucketOptions{
		BucketName: testBucketName,
		BucketSettings: BucketSettings{
			MutableBucketSettings: MutableBucketSettings{
				RAMQuotaMB:         100,
				EvictionPolicy:     EvictionPolicyTypeValueOnly,
				CompressionMode:    CompressionModePassive,
				DurabilityMinLevel: DurabilityLevelNone,
			},
			ConflictResolutionType: ConflictResolutionTypeSequenceNumber,
			BucketType:             BucketTypeCouchbase,
			StorageBackend:         StorageBackendCouchstore,
			ReplicaIndex:           true,
		},
	})
	require.NoError(t, err)

	hlpr := EnsureBucketHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  testBucketName,
		BucketUUID:  "",
		WantMissing: false,
	}

	require.Eventually(t, func() bool {
		res, err := hlpr.Poll(ctx, &EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)

	err = mgmt.DeleteBucket(ctx, &DeleteBucketOptions{
		BucketName: testBucketName,
	})
	require.NoError(t, err)

	hlprDel := EnsureBucketHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  testBucketName,
		BucketUUID:  "",
		WantMissing: true,
	}

	require.Eventually(t, func() bool {
		res, err := hlprDel.Poll(ctx, &EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 500*time.Millisecond)
}
