package cbauthx_test

import (
	"context"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbauthx"
	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
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

func TestCbauthInvalidations(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfOlderServerVersion(t, "7.2.0")

	ctx := context.Background()
	logger := testutils.MakeTestLogger(t)

	user1 := uuid.NewString()
	user2 := uuid.NewString()

	auth, err := cbauthx.NewCbAuth(ctx, &cbauthx.CbAuthConfig{
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

	mgmt := cbmgmtx.Management{
		Transport: http.DefaultTransport,
		UserAgent: "stg",
		Endpoint:  "http://" + testutilsint.TestOpts.HTTPAddrs[0],
		Auth: &cbhttpx.BasicAuth{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
	}

	// check that user1 is handled properly before creation
	_, err = auth.CheckUserPass(ctx, user1, "password")
	assert.ErrorIs(t, err, cbauthx.ErrInvalidAuth)

	_, err = auth.CheckUserPass(ctx, user1, "badpassword")
	assert.ErrorIs(t, err, cbauthx.ErrInvalidAuth)

	// create user1 and user2
	err = mgmt.UpsertUser(ctx, &cbmgmtx.UpsertUserOptions{
		Username:    user1,
		DisplayName: user1,
		Password:    "password1",
		Roles:       []string{"ro_admin"},
	})
	require.NoError(t, err)

	err = mgmt.UpsertUser(ctx, &cbmgmtx.UpsertUserOptions{
		Username:    user2,
		DisplayName: user2,
		Password:    "password2",
		Roles:       []string{"ro_admin"},
	})
	require.NoError(t, err)

	// check that user1 is handled properly after creation
	_, err = auth.CheckUserPass(ctx, user1, "password1")
	assert.NoError(t, err)

	_, err = auth.CheckUserPass(ctx, user1, "badpassword1")
	assert.ErrorIs(t, err, cbauthx.ErrInvalidAuth)

	// check that user2 is handled properly after creation
	_, err = auth.CheckUserPass(ctx, user2, "password2")
	assert.NoError(t, err)

	_, err = auth.CheckUserPass(ctx, user2, "badpassword2")
	assert.ErrorIs(t, err, cbauthx.ErrInvalidAuth)

	// delete user1 and user2
	err = mgmt.DeleteUser(ctx, &cbmgmtx.DeleteUserOptions{
		Username: user1,
	})
	require.NoError(t, err)

	err = mgmt.DeleteUser(ctx, &cbmgmtx.DeleteUserOptions{
		Username: user2,
	})
	require.NoError(t, err)

	// user1 and user2 should both fail with bad passwords before propagation
	_, err = auth.CheckUserPass(ctx, user1, "badpassword1")
	assert.ErrorIs(t, err, cbauthx.ErrInvalidAuth)

	_, err = auth.CheckUserPass(ctx, user2, "badpassword2")
	assert.ErrorIs(t, err, cbauthx.ErrInvalidAuth)

	// user1 should propagate and be invalidated pretty quickly
	assert.Eventually(t, func() bool {
		_, err = auth.CheckUserPass(ctx, user1, "password1")
		return err == cbauthx.ErrInvalidAuth
	}, 3*time.Second, 100*time.Millisecond)

	// user2 should propagate and be invalidated pretty quickly
	assert.Eventually(t, func() bool {
		_, err = auth.CheckUserPass(ctx, user2, "password2")
		return err == cbauthx.ErrInvalidAuth
	}, 3*time.Second, 100*time.Millisecond)

	// clean up
	_ = auth.Close()
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

func TestCbauthRebalanceOutDino(t *testing.T) {
	testutilsint.SkipIfNoDinoCluster(t)
	testutilsint.SkipIfOlderServerVersion(t, "7.2.0")

	ctx := context.Background()
	logger := testutils.MakeTestLogger(t)

	// start dino testing
	dino := testutilsint.StartDinoTesting(t, true)

	// add a new node to the cluster to test with
	nodeID := dino.AddNode()
	nodeIP := dino.GetNodeIP(nodeID)

	// fetch the list of nodes (after we added one)
	nodes := testutilsint.GetTestNodes(t)
	log.Printf("Nodes: %+v %+v", nodes, nodeIP)

	// identify the node endpoint for our test node
	var testNodeEndpoint string
	for _, target := range nodes {
		if target.Hostname == nodeIP {
			testNodeEndpoint = target.NsEndpoint()
		}
	}
	require.NotEmpty(t, testNodeEndpoint)

	// list the rest of the nodes
	var allEndpoints []string
	for _, node := range nodes {
		allEndpoints = append(allEndpoints, node.NsEndpoint())
	}

	// We create with a single host, and then reconfigure into multiple to
	// force it to be connected to the block host initially.
	auth, err := cbauthx.NewCbAuth(ctx, &cbauthx.CbAuthConfig{
		Endpoints: []string{
			testNodeEndpoint,
		},
		Username:    testutilsint.TestOpts.Username,
		Password:    testutilsint.TestOpts.Password,
		ClusterUuid: "",
	}, &cbauthx.CbAuthOptions{
		Logger:            logger.Named("auth"),
		ServiceName:       "stgm",
		UserAgent:         "cng-test",
		HeartbeatInterval: 3 * time.Second,
		HeartbeatTimeout:  5 * time.Second,
		LivenessTimeout:   6 * time.Second,
		ConnectTimeout:    3 * time.Second,
	})
	require.NoError(t, err)

	err = auth.Reconfigure(&cbauthx.CbAuthConfig{
		Endpoints:   allEndpoints,
		Username:    testutilsint.TestOpts.Username,
		Password:    testutilsint.TestOpts.Password,
		ClusterUuid: "",
	})
	require.NoError(t, err)

	// do not check any users so that our cache is empty

	// remove the node from the cluster
	dino.RemoveNode(nodeID)

	// Check that a valid user works
	userInfo, err := auth.CheckUserPass(ctx,
		testutilsint.TestOpts.Username,
		testutilsint.TestOpts.Password)
	require.NoError(t, err)
	require.NotEmpty(t, userInfo)

	// clean up
	_ = auth.Close()
}
