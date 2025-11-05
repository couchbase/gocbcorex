package cbqueryx_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEnsureQuery(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()
	transport := http.DefaultTransport

	nodes := testutilsint.GetTestNodes(t)
	queryNodes := nodes.Select(func(node *testutilsint.NodeTarget) bool {
		return node.QueryPort > 0
	})
	if len(queryNodes) == 0 {
		t.Skip("Skipping test, no query nodes")
	}

	var targets []cbqueryx.NodeTarget
	for _, queryNode := range queryNodes {
		targets = append(targets, cbqueryx.NodeTarget{
			Endpoint: queryNode.QueryEndpoint(),
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		})
	}

	// we intentionally use the last target that will be polled as the node
	// to create the index with so we don't unintentionally give additional
	// time for nodes to sync their configuration
	query := cbqueryx.Query{
		Logger:    testutils.MakeTestLogger(t),
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  targets[len(targets)-1].Endpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
	}

	idxName := uuid.NewString()[:6]
	res, err := query.Query(ctx, &cbqueryx.QueryOptions{
		Statement: fmt.Sprintf(
			"CREATE INDEX `%s` On `%s`._default._default(test)",
			idxName,
			testutilsint.TestOpts.BucketName,
		),
	})
	if errors.Is(err, cbqueryx.ErrBuildAlreadyInProgress) {
		// the build is delayed, we need to wait
	} else {
		require.NoError(t, err)

		for res.HasMoreRows() {
			_, err := res.ReadRow()
			require.NoError(t, err)
		}
	}

	var hlprLock sync.Mutex
	hlpr := cbqueryx.EnsureIndexHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:     testutilsint.TestOpts.BucketName,
		ScopeName:      "_default",
		CollectionName: "_default",
		IndexName:      idxName,
	}

	require.Eventually(t, func() bool {
		hlprLock.Lock()
		defer hlprLock.Unlock()

		res, err := hlpr.PollCreated(ctx, &cbqueryx.EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 60*time.Second, 1*time.Second)

	res, err = query.Query(ctx, &cbqueryx.QueryOptions{
		Statement: fmt.Sprintf(
			"DROP INDEX `%s` ON `%s`.`_default`.`_default`",
			idxName,
			testutilsint.TestOpts.BucketName,
		),
	})
	require.NoError(t, err)

	for res.HasMoreRows() {
		_, err := res.ReadRow()
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		hlprLock.Lock()
		defer hlprLock.Unlock()

		res, err := hlpr.PollDropped(ctx, &cbqueryx.EnsureIndexPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 60*time.Second, 1*time.Second)
}
