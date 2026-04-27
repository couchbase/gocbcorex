package cbmgmtx_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterInfoHelper(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()

	nodes := testutilsint.GetTestNodes(t)

	var targets []cbmgmtx.NodeTarget
	for _, node := range nodes {
		targets = append(targets, cbmgmtx.NodeTarget{
			Endpoint: node.NsEndpoint(),
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		})
	}

	hlpr := cbmgmtx.ClusterInfoHelper{
		Logger:    testutils.MakeTestLogger(t),
		UserAgent: "gocbcorex test",
	}

	resp, err := hlpr.FetchAll(ctx, &cbmgmtx.GetAggregatedClusterInfoOptions{
		Transport: http.DefaultTransport,
		Targets:   targets,
	})
	require.NoError(t, err)

	assert.NotEmpty(t, resp.Uuid)
	assert.Len(t, resp.Nodes, len(targets))

	// in a normal test cluster all nodes should be the same edition,
	// so exactly one of AllEnterprise or AllCommunity should be true.
	assert.True(t, resp.AllEnterprise || resp.AllCommunity,
		"expected all nodes to be the same edition in a test cluster")
	assert.False(t, resp.AllEnterprise && resp.AllCommunity,
		"AllEnterprise and AllCommunity cannot both be true")

	for _, node := range resp.Nodes {
		assert.NotEmpty(t, node.Endpoint)
		assert.Equal(t, resp.AllEnterprise, node.IsEnterprise)
	}
}
