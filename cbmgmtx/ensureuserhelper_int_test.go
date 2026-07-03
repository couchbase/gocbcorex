package cbmgmtx_test

import (
	"context"
	"log"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEnsureUserDino(t *testing.T) {
	testutilsint.SkipIfNoDinoCluster(t)

	ctx := context.Background()
	transport := http.DefaultTransport
	testUsername := "testuser-" + uuid.NewString()[:6]

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
		Auth: &cbhttpx.BasicAuth{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
	}

	createTestUser := func() *cbmgmtx.PasswordChangedMarker {
		log.Printf("creating the user")
		res, err := mgmt.UpsertUser(ctx, &cbmgmtx.UpsertUserOptions{
			Username:    testUsername,
			DisplayName: testUsername,
			Password:    "password1",
			Roles:       []string{"ro_admin"},
		})
		require.NoError(t, err)

		return res.PreviousPasswordChanged
	}

	changeTestUserPassword := func() *cbmgmtx.PasswordChangedMarker {
		log.Printf("changing the user's password")
		res, err := mgmt.UpsertUser(ctx, &cbmgmtx.UpsertUserOptions{
			Username:    testUsername,
			DisplayName: testUsername,
			Password:    "password2",
			Roles:       []string{"ro_admin"},
		})
		require.NoError(t, err)

		return res.PreviousPasswordChanged
	}

	deleteTestUser := func() {
		log.Printf("deleting the user")
		err := mgmt.DeleteUser(ctx, &cbmgmtx.DeleteUserOptions{
			Username: testUsername,
		})
		require.NoError(t, err)
	}

	// start dino testing
	dino := testutilsint.StartDinoTesting(t, true)

	// block access to the first endpoint
	dino.BlockNodeTraffic(blockHost)

	// create the test user. Since this is a brand new user, the returned
	// baseline marker is the zero-value marker (there is no prior password
	// change), so waiting on it also confirms that the newly-created user
	// is visible everywhere.
	createMarker := createTestUser()

	var syncCreate sync.Mutex
	hlprCreate := cbmgmtx.EnsureUserHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		Username:             testUsername,
		WantMissing:          false,
		SincePasswordChanged: createMarker,
	}

	// the first couple of polls should fail, since a node is unavailable
	require.Never(t, func() bool {
		syncCreate.Lock()
		defer syncCreate.Unlock()

		res, err := hlprCreate.Poll(ctx, &cbmgmtx.EnsureUserPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 5*time.Second, 500*time.Millisecond)

	// stop blocking traffic to the node
	dino.AllowTraffic(blockHost)

	// we should see that the polls eventually succeed
	require.Eventually(t, func() bool {
		syncCreate.Lock()
		defer syncCreate.Unlock()

		res, err := hlprCreate.Poll(ctx, &cbmgmtx.EnsureUserPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 90*time.Second, 1*time.Second)

	// now lets block traffic again before we change the password
	dino.BlockNodeTraffic(blockHost)

	// change the user's password, capturing the baseline from immediately
	// before the change so we can confirm this specific change propagates,
	// rather than just the user record generally
	passwordMarker := changeTestUserPassword()

	var syncPassword sync.Mutex
	hlprPassword := cbmgmtx.EnsureUserHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		Username:             testUsername,
		WantMissing:          false,
		SincePasswordChanged: passwordMarker,
	}

	// the first couple of polls should fail, since a node is unavailable
	require.Never(t, func() bool {
		syncPassword.Lock()
		defer syncPassword.Unlock()

		res, err := hlprPassword.Poll(ctx, &cbmgmtx.EnsureUserPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 5*time.Second, 500*time.Millisecond)

	// stop blocking traffic to the node
	dino.AllowTraffic(blockHost)

	// we should see that the polls eventually succeed
	require.Eventually(t, func() bool {
		syncPassword.Lock()
		defer syncPassword.Unlock()

		res, err := hlprPassword.Poll(ctx, &cbmgmtx.EnsureUserPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)

	// now lets block traffic again before we delete
	dino.BlockNodeTraffic(blockHost)

	// delete the user
	deleteTestUser()

	var syncDel sync.Mutex
	hlprDel := cbmgmtx.EnsureUserHelper{
		Logger:    testutils.MakeTestLogger(t),
		UserAgent: "useragent",

		Username:    testUsername,
		WantMissing: true,
	}

	// the first couple of polls should fail, since a node is unavailable
	require.Never(t, func() bool {
		syncDel.Lock()
		defer syncDel.Unlock()

		res, err := hlprDel.Poll(ctx, &cbmgmtx.EnsureUserPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 5*time.Second, 500*time.Millisecond)

	// stop blocking traffic to the node
	dino.AllowTraffic(blockHost)

	// we should see that the polls eventually succeed
	require.Eventually(t, func() bool {
		syncDel.Lock()
		defer syncDel.Unlock()

		res, err := hlprDel.Poll(ctx, &cbmgmtx.EnsureUserPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 500*time.Millisecond)
}
