package testutils

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/couchbase/gocbcorex/cbmgmtx"

	"github.com/stretchr/testify/require"
)

type TestNodeTarget struct {
	Hostname       string
	NsPort         uint16
	QueryPort      uint16
	SearchPort     uint16
	GSIPort        uint16
	IsOrchestrator bool
}

func getOrchestratorNsAddr(t *testing.T) string {
	mgmt := cbmgmtx.Management{
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  "http://" + TestOpts.HTTPAddrs[0],
		Username:  TestOpts.Username,
		Password:  TestOpts.Password,
	}

	clusterInfo, err := mgmt.GetTerseClusterInfo(context.Background(), &cbmgmtx.GetTerseClusterConfigOptions{})
	require.NoError(t, err)

	config, err := mgmt.GetClusterConfig(context.Background(), &cbmgmtx.GetClusterConfigOptions{})
	require.NoError(t, err)

	var otpNodes []string
	for _, node := range config.Nodes {
		if node.OTPNode == clusterInfo.Orchestrator {
			return node.Hostname
		}
		otpNodes = append(otpNodes, node.OTPNode)
	}

	require.Fail(t, "failed to find orchestrator nsaddr", "found %s nodes in config, cluster info: %v", strings.Join(otpNodes, ""), clusterInfo)
	return ""
}

func getTestNodes(t *testing.T) []TestNodeTarget {
	mgmt := cbmgmtx.Management{
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  "http://" + TestOpts.HTTPAddrs[0],
		Username:  TestOpts.Username,
		Password:  TestOpts.Password,
	}

	config, err := mgmt.GetTerseClusterConfig(context.Background(), &cbmgmtx.GetTerseClusterConfigOptions{})
	require.NoError(t, err)

	orchestratorNsAddr := getOrchestratorNsAddr(t)

	var nodes []TestNodeTarget
	for _, nodeExt := range config.NodesExt {
		nsAddress := fmt.Sprintf("%s:%d", nodeExt.Hostname, nodeExt.Services.Mgmt)

		nodes = append(nodes, TestNodeTarget{
			Hostname:       nodeExt.Hostname,
			NsPort:         nodeExt.Services.Mgmt,
			QueryPort:      nodeExt.Services.N1ql,
			SearchPort:     nodeExt.Services.Fts,
			GSIPort:        nodeExt.Services.GSI,
			IsOrchestrator: nsAddress == orchestratorNsAddr,
		})
	}

	return nodes
}

type TestService uint8

const (
	TestServiceNs TestService = iota + 1
	TestServiceQuery
	TestServiceSearch
)

func SelectNodeToBlock(t *testing.T, service TestService) (string, string, []TestNodeTarget) {
	nodes := getTestNodes(t)

	// we intentionally use the last target that will be polled as the node
	// to create the bucket with so we don't unintentionally give additional
	// time for nodes to sync their configuration, additionally we ensure that
	// we do not select the orchestrator as the node to block since that incurs
	// a huge time penalty to the test waiting for orchestrator election.
	var execEndpoint string
	var blockHost string
	for _, node := range nodes {
		if service == TestServiceQuery {
			// We have to use the query node which does not also have indexer on it.
			if blockHost == "" && node.QueryPort != 0 && node.GSIPort == 0 {
				blockHost = node.Hostname
			}
		} else if service == TestServiceSearch {
			if blockHost == "" && node.SearchPort != 0 {
				blockHost = node.Hostname
			}
		} else {
			// select the first non-orchestrator node to block
			if blockHost == "" && !node.IsOrchestrator {
				blockHost = node.Hostname
			}
		}

		// we always want to execute on the last node of the cluster
		var port uint16
		switch service {
		case TestServiceNs:
			port = node.NsPort
		case TestServiceQuery:
			port = node.QueryPort
		case TestServiceSearch:
			port = node.SearchPort
		}
		execEndpoint = fmt.Sprintf("http://%s:%d", node.Hostname, port)
	}

	log.Printf("nodes:")
	for _, node := range nodes {
		log.Printf("  %s:%d (orchestrator: %t)", node.Hostname, node.NsPort, node.IsOrchestrator)
	}
	log.Printf("execution endpoint: %s", execEndpoint)
	log.Printf("blocked host: %s", blockHost)

	return blockHost, execEndpoint, nodes
}

func DisableAutoFailover(t *testing.T) {
	mgmt := cbmgmtx.Management{
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  "http://" + TestOpts.HTTPAddrs[0],
		Username:  TestOpts.Username,
		Password:  TestOpts.Password,
	}

	err := mgmt.DisableAutoFailover(context.Background(), &cbmgmtx.DisableAutoFailoverOptions{})
	require.NoError(t, err)
}
