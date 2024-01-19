package testutilsint

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"

	"github.com/stretchr/testify/require"
)

type NodeTarget struct {
	Hostname       string
	NsPort         uint16
	QueryPort      uint16
	SearchPort     uint16
	GSIPort        uint16
	IsOrchestrator bool
}

func (t NodeTarget) NsEndpoint() string {
	return fmt.Sprintf("http://%s:%d", t.Hostname, t.NsPort)
}

func (t NodeTarget) QueryEndpoint() string {
	return fmt.Sprintf("http://%s:%d", t.Hostname, t.QueryPort)
}

func (t NodeTarget) SearchEndpoint() string {
	return fmt.Sprintf("http://%s:%d", t.Hostname, t.SearchPort)
}

func (t NodeTarget) String() string {
	return fmt.Sprintf("%s (ns:%d, query:%d, search:%d, gsi:%d, orch:%t)",
		t.Hostname,
		t.NsPort,
		t.QueryPort,
		t.SearchPort,
		t.GSIPort,
		t.IsOrchestrator)
}

type NodeTargetList []*NodeTarget

func (l NodeTargetList) Select(fn func(node *NodeTarget) bool) []*NodeTarget {
	var out []*NodeTarget
	for _, node := range l {
		if fn(node) {
			out = append(out, node)
		}
	}
	return out
}

func (l NodeTargetList) SelectFirst(t *testing.T, fn func(node *NodeTarget) bool) *NodeTarget {
	for i := 0; i < len(l); i++ {
		if fn(l[i]) {
			return l[i]
		}
	}
	require.Fail(t, "failed to select a node")
	return nil
}

func (l NodeTargetList) SelectLast(t *testing.T, fn func(node *NodeTarget) bool) *NodeTarget {
	for i := len(l) - 1; i >= 0; i-- {
		if fn(l[i]) {
			return l[i]
		}
	}
	require.Fail(t, "failed to select a node")
	return nil
}

func getOrchestratorNsAddr(t *testing.T) string {
	clusterInfo, err := getTestMgmt().GetTerseClusterInfo(context.Background(), &cbmgmtx.GetTerseClusterConfigOptions{})
	require.NoError(t, err)

	if clusterInfo.Orchestrator == "undefined" {
		// sometimes ns_server will return an orchestrator otp name of "undefined".
		// so we loop here to wait until ns_server figures out who the orchestrator is.
		time.Sleep(1 * time.Second)
		return getOrchestratorNsAddr(t)
	}

	config, err := getTestMgmt().GetClusterConfig(context.Background(), &cbmgmtx.GetClusterConfigOptions{})
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

func GetTestNodes(t *testing.T) NodeTargetList {
	config, err := getTestMgmt().GetTerseClusterConfig(context.Background(), &cbmgmtx.GetTerseClusterConfigOptions{})
	require.NoError(t, err)

	orchestratorNsAddr := getOrchestratorNsAddr(t)

	var nodes []*NodeTarget
	for _, nodeExt := range config.NodesExt {
		nsAddress := fmt.Sprintf("%s:%d", nodeExt.Hostname, nodeExt.Services.Mgmt)

		nodes = append(nodes, &NodeTarget{
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
