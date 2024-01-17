package cbauthx_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbauthx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCbAuthBasicSlow(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfOlderServerVersion(t, "7.2.0")

	logger := zap.Must(zap.NewDevelopment())

	// Connecting to an invalid host should return an error
	_, err := cbauthx.NewCbAuth(context.Background(), &cbauthx.CbAuthConfig{
		Endpoints: []string{
			"http://localhost:9999",
		},
		Username:    testutilsint.TestOpts.Username,
		Password:    testutilsint.TestOpts.Password,
		ClusterUuid: "",
	}, &cbauthx.CbAuthOptions{
		Logger:            logger,
		ServiceName:       "stg",
		UserAgent:         "cng-test",
		HeartbeatInterval: 3 * time.Second,
		HeartbeatTimeout:  5 * time.Second,
		LivenessTimeout:   6 * time.Second,
		ConnectTimeout:    3 * time.Second,
	})
	require.Error(t, err)

	auth, err := cbauthx.NewCbAuth(context.Background(), &cbauthx.CbAuthConfig{
		Endpoints: []string{
			"http://" + testutilsint.TestOpts.HTTPAddrs[0],
		},
		Username:    testutilsint.TestOpts.Username,
		Password:    testutilsint.TestOpts.Password,
		ClusterUuid: "",
	}, &cbauthx.CbAuthOptions{
		Logger:            logger,
		ServiceName:       "stg",
		UserAgent:         "cng-test",
		HeartbeatInterval: 3 * time.Second,
		HeartbeatTimeout:  5 * time.Second,
		LivenessTimeout:   6 * time.Second,
		ConnectTimeout:    3 * time.Second,
	})
	require.NoError(t, err)

	// Check that a valid user works
	userInfo, err := auth.CheckUserPass(context.Background(),
		testutilsint.TestOpts.Username,
		testutilsint.TestOpts.Password)
	require.NoError(t, err)
	assert.Subset(t, []string{"admin", "local"}, []string{userInfo.Domain})

	// Check that an invalid user fails
	_, err = auth.CheckUserPass(context.Background(), "baduser", "badpass")
	require.ErrorIs(t, err, cbauthx.ErrInvalidAuth)

	// Reconfigure to something invalid
	err = auth.Reconfigure(&cbauthx.CbAuthConfig{
		Endpoints: []string{
			"http://localhost:9999",
		},
		Username:    testutilsint.TestOpts.Username,
		Password:    testutilsint.TestOpts.Password,
		ClusterUuid: "",
	})
	require.NoError(t, err)

	// The reconfigure should be internally failing, leading to a
	// continued ability to keep using the existing source.
	_, err = auth.CheckUserPass(context.Background(),
		testutilsint.TestOpts.Username,
		testutilsint.TestOpts.Password)
	require.NoError(t, err)

	_, err = auth.CheckUserPass(context.Background(), "baduser", "badpass")
	require.ErrorIs(t, err, cbauthx.ErrInvalidAuth)

	// Wait 7 seconds for our liveness to be invalidated.
	time.Sleep(7 * time.Second)

	// Now bad things should start happening...
	_, err = auth.CheckUserPass(context.Background(),
		testutilsint.TestOpts.Username,
		testutilsint.TestOpts.Password)
	require.Error(t, err)
	// should fail with reference to cbauth being unavailable
	assert.ErrorContains(t, err, "cbauth unavailability")
	// should include the details on why its unavailable
	assert.ErrorContains(t, err, "failed to dial")

	_, err = auth.CheckUserPass(context.Background(), "baduser", "badpass")
	require.Error(t, err)
	// should fail with reference to cbauth being unavailable
	assert.ErrorContains(t, err, "cbauth unavailability")
	// should include the details on why its unavailable
	assert.ErrorContains(t, err, "failed to dial")

	auth.Close()
}

func TestCbauthFailuresDino(t *testing.T) {
	testutilsint.SkipIfNoDinoCluster(t)
	testutilsint.SkipIfOlderServerVersion(t, "7.2.0")

	ctx := context.Background()
	logger := testutils.MakeTestLogger(t)
	nodes := testutilsint.GetTestNodes(t)

	blockNode := nodes.SelectFirst(t, func(node *testutilsint.NodeTarget) bool {
		return !node.IsOrchestrator
	})

	blockHost := blockNode.Hostname
	blockEndpoint := blockNode.NsEndpoint()

	log.Printf("nodes:")
	for _, node := range nodes {
		log.Printf("  %s", node)
	}
	log.Printf("test endpoint: %s", blockEndpoint)

	var allEndpoints []string
	for _, node := range nodes {
		allEndpoints = append(allEndpoints, node.NsEndpoint())
	}

	authOne, err := cbauthx.NewCbAuth(context.Background(), &cbauthx.CbAuthConfig{
		Endpoints: []string{
			blockEndpoint,
		},
		Username:    testutilsint.TestOpts.Username,
		Password:    testutilsint.TestOpts.Password,
		ClusterUuid: "",
	}, &cbauthx.CbAuthOptions{
		Logger:            logger.Named("single"),
		ServiceName:       "stgs",
		UserAgent:         "cng-test",
		HeartbeatInterval: 3 * time.Second,
		HeartbeatTimeout:  5 * time.Second,
		LivenessTimeout:   6 * time.Second,
		ConnectTimeout:    3 * time.Second,
	})
	require.NoError(t, err)

	// We create with a single host, and then reconfigure into multiple to
	// force it to be connected to the block host initially.
	authMulti, err := cbauthx.NewCbAuth(context.Background(), &cbauthx.CbAuthConfig{
		Endpoints: []string{
			blockEndpoint,
		},
		Username:    testutilsint.TestOpts.Username,
		Password:    testutilsint.TestOpts.Password,
		ClusterUuid: "",
	}, &cbauthx.CbAuthOptions{
		Logger:            logger.Named("multi"),
		ServiceName:       "stgm",
		UserAgent:         "cng-test",
		HeartbeatInterval: 3 * time.Second,
		HeartbeatTimeout:  5 * time.Second,
		LivenessTimeout:   6 * time.Second,
		ConnectTimeout:    3 * time.Second,
	})
	require.NoError(t, err)

	err = authMulti.Reconfigure(&cbauthx.CbAuthConfig{
		Endpoints:   allEndpoints,
		Username:    testutilsint.TestOpts.Username,
		Password:    testutilsint.TestOpts.Password,
		ClusterUuid: "",
	})
	require.NoError(t, err)

	// prime the cache
	_, err = authOne.CheckUserPass(ctx, testutilsint.TestOpts.Username, testutilsint.TestOpts.Password)
	assert.NoError(t, err)

	_, err = authMulti.CheckUserPass(ctx, testutilsint.TestOpts.Username, testutilsint.TestOpts.Password)
	assert.NoError(t, err)

	// start dino testing
	dino := testutilsint.StartDinoTesting(t, true)

	// block access to the test endpoint
	dino.BlockAllTraffic(blockHost)

	// wait for our liveness timeout to expire
	// we wait longer than the actual liveness timeout because the iptables rules
	// that cbdinocluster uses will still allow one last heartbeat to be received
	// before the connection ends up being dropped due to lack of connectivity.
	time.Sleep(10 * time.Second)

	// single-host cbauth should be returning liveness errors
	_, err = authOne.CheckUserPass(ctx, testutilsint.TestOpts.Username, testutilsint.TestOpts.Password)
	assert.ErrorContains(t, err, "cannot check auth due to cbauth unavailability")

	// single-host cbauth invalid should fail for the same reason
	_, err = authOne.CheckUserPass(ctx, "invalid", "invalid")
	assert.ErrorContains(t, err, "cannot check auth due to cbauth unavailability")

	// multi-host cbauth should be still working, thanks to it selecting a new host
	_, err = authMulti.CheckUserPass(ctx, testutilsint.TestOpts.Username, testutilsint.TestOpts.Password)
	assert.NoError(t, err)

	// multi-host cbauth invalid should just fail
	_, err = authMulti.CheckUserPass(ctx, "invalid", "invalid")
	assert.ErrorIs(t, err, cbauthx.ErrInvalidAuth)

	// stop blocking traffic to the node
	dino.AllowTraffic(blockHost)

	_ = authOne.Close()
	_ = authMulti.Close()
}
