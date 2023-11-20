package cbqueryx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEnsureQuery(t *testing.T) {
	testutils.SkipIfShortTest(t)

	ctx := context.Background()
	transport := http.DefaultTransport

	config, err := cbmgmtx.Management{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  "http://" + testutils.TestOpts.HTTPAddrs[0],
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}.GetTerseClusterConfig(ctx, &cbmgmtx.GetTerseClusterConfigOptions{
		OnBehalfOf: nil,
	})
	require.NoError(t, err)

	var targets []NodeTarget
	for _, nodeExt := range config.NodesExt {
		if nodeExt.Services.N1ql > 0 {
			targets = append(targets, NodeTarget{
				Endpoint: fmt.Sprintf("http://%s:%d", nodeExt.Hostname, nodeExt.Services.N1ql),
				Username: testutils.TestOpts.Username,
				Password: testutils.TestOpts.Password,
			})
		}
	}
	if len(targets) == 0 {
		t.Skip("Skipping test, no query nodes")
	}

	// we intentionally use the last target that will be polled as the node
	// to create the index with so we don't unintentionally give additional
	// time for nodes to sync their configuration
	query := Query{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  targets[len(targets)-1].Endpoint,
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}

	idxName := uuid.NewString()[:6]
	res, err := query.Query(ctx, &Options{
		Statement: fmt.Sprintf(
			"CREATE INDEX `%s` On `%s`._default._default(test)",
			idxName,
			testutils.TestOpts.BucketName,
		),
	})
	if err != nil {
		if !errors.Is(err, ErrIndexExists) {
			t.Fatalf("Failed to create index: %v", err)
		}
	}

	for res.HasMoreRows() {
	}

	hlpr := EnsureIndexHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:     testutils.TestOpts.BucketName,
		ScopeName:      "_default",
		CollectionName: "_default",
		IndexName:      idxName,
	}

	require.Eventually(t, func() bool {
		res, err := hlpr.PollCreated(ctx, &EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)

	res, err = query.Query(ctx, &Options{
		Statement: fmt.Sprintf(
			"DROP INDEX `%s` ON `%s`.`_default`.`_default`",
			idxName,
			testutils.TestOpts.BucketName,
		),
	})
	require.NoError(t, err)

	for res.HasMoreRows() {
	}
	require.Eventually(t, func() bool {
		res, err := hlpr.PollDropped(ctx, &EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)
}
