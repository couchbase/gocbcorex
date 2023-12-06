package cbmgmtx

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func getOrchestratorNsAddr(t *testing.T) string {
	mgmt := Management{
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  "http://" + testutils.TestOpts.HTTPAddrs[0],
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}

	clusterInfo, err := mgmt.GetTerseClusterInfo(context.Background(), &GetTerseClusterConfigOptions{})
	require.NoError(t, err)

	config, err := mgmt.GetClusterConfig(context.Background(), &GetClusterConfigOptions{})
	require.NoError(t, err)

	for _, node := range config.Nodes {
		if node.OTPNode == clusterInfo.Orchestrator {
			return node.Hostname
		}
	}

	require.Fail(t, "failed to find orchestrator nsaddr")
	return ""
}

type testNodeTarget struct {
	Hostname       string
	NsPort         uint16
	IsOrchestrator bool
}

func getTestNodes(t *testing.T) []testNodeTarget {
	mgmt := Management{
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  "http://" + testutils.TestOpts.HTTPAddrs[0],
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}

	config, err := mgmt.GetTerseClusterConfig(context.Background(), &GetTerseClusterConfigOptions{})
	require.NoError(t, err)

	orchestratorNsAddr := getOrchestratorNsAddr(t)

	var nodes []testNodeTarget
	for _, nodeExt := range config.NodesExt {
		nsAddress := fmt.Sprintf("%s:%d", nodeExt.Hostname, nodeExt.Services.Mgmt)

		nodes = append(nodes, testNodeTarget{
			Hostname:       nodeExt.Hostname,
			NsPort:         nodeExt.Services.Mgmt,
			IsOrchestrator: nsAddress == orchestratorNsAddr,
		})
	}

	return nodes
}

func testNodesToNodeTargets(t *testing.T, nodes []testNodeTarget) []NodeTarget {
	var targets []NodeTarget
	for _, node := range nodes {
		targets = append(targets, NodeTarget{
			Endpoint: fmt.Sprintf("http://%s:%d", node.Hostname, node.NsPort),
			Username: testutils.TestOpts.Username,
			Password: testutils.TestOpts.Password,
		})
	}
	return targets
}

func TestEnsureBucketDino(t *testing.T) {
	testutils.SkipIfNoDinoCluster(t)

	ctx := context.Background()
	transport := http.DefaultTransport
	testBucketName := "testbucket-" + uuid.NewString()[:6]

	nodes := getTestNodes(t)
	targets := testNodesToNodeTargets(t, nodes)

	// we intentionally use the last target that will be polled as the node
	// to create the bucket with so we don't unintentionally give additional
	// time for nodes to sync their configuration, additionally we ensure that
	// we do not select the orchestrator as the node to block since that incurs
	// a huge time penalty to the test waiting for orchestrator election.
	var execEndpoint string
	var blockHost string
	for _, node := range nodes {
		// select the first non-orchestrator node to block
		if blockHost == "" && !node.IsOrchestrator {
			blockHost = node.Hostname
		}

		// we always want to execute on the last node of the cluster
		execEndpoint = fmt.Sprintf("http://%s:%d", node.Hostname, node.NsPort)
	}

	log.Printf("nodes:")
	for _, node := range nodes {
		log.Printf("  %s:%d (orchestrator: %t)", node.Hostname, node.NsPort, node.IsOrchestrator)
	}
	log.Printf("execution endpoint: %s", execEndpoint)
	log.Printf("blocked host: %s", blockHost)

	mgmt := Management{
		Transport: transport,
		UserAgent: "useragent",
		Endpoint:  execEndpoint,
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}

	createTestBucket := func() {
		require.Eventually(t, func() bool {
			log.Printf("attempting to create the bucket")
			err := mgmt.CreateBucket(ctx, &CreateBucketOptions{
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
			if err != nil {
				log.Printf("bucket creation failed with error: %s", err)
				return false
			}

			return true
		}, 120*time.Second, 1*time.Second)
	}

	deleteTestBucket := func() {
		require.Eventually(t, func() bool {
			log.Printf("attempting to delete the bucket")
			err := mgmt.DeleteBucket(ctx, &DeleteBucketOptions{
				BucketName: testBucketName,
			})
			if err != nil {
				log.Printf("bucket deletion failed with error: %s", err)
				return false
			}

			return true
		}, 120*time.Second, 1*time.Second)
	}

	// we set up a cleanup function to ensure that traffic is allowed so we
	// don't accidentally leave the cluster in a randomly unusable state
	// if an error occurs during the test.
	t.Cleanup(func() {
		testutils.DinoAllowTraffic(t, blockHost)
	})

	// block access to the first endpoint
	testutils.DinoBlockTraffic(t, blockHost)

	// create the test bucket
	createTestBucket()

	hlpr := EnsureBucketHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  testBucketName,
		BucketUUID:  "",
		WantMissing: false,
	}

	// the first couple of polls should fail, since a node is unavailable
	require.Never(t, func() bool {
		res, err := hlpr.Poll(ctx, &EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 5*time.Second, 500*time.Millisecond)

	// stop blocking traffic to the node
	testutils.DinoAllowTraffic(t, blockHost)

	// we should see that the polls eventually succeed
	require.Eventually(t, func() bool {
		res, err := hlpr.Poll(ctx, &EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 1*time.Second)

	// now lets block traffic again before we delete
	testutils.DinoBlockTraffic(t, blockHost)

	// delete the bucket
	deleteTestBucket()

	hlprDel := EnsureBucketHelper{
		Logger:     testutils.MakeTestLogger(t),
		UserAgent:  "useragent",
		OnBehalfOf: nil,

		BucketName:  testBucketName,
		BucketUUID:  "",
		WantMissing: true,
	}

	// the first couple of polls should fail, since a node is unavailable
	require.Never(t, func() bool {
		res, err := hlprDel.Poll(ctx, &EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 5*time.Second, 500*time.Millisecond)

	// stop blocking traffic to the node
	testutils.DinoAllowTraffic(t, blockHost)

	// we should see that the polls eventually succeed
	require.Eventually(t, func() bool {
		res, err := hlprDel.Poll(ctx, &EnsureBucketPollOptions{
			Transport: transport,
			Targets:   targets,
		})
		require.NoError(t, err)

		return res
	}, 30*time.Second, 500*time.Millisecond)
}
