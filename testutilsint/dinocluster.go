package testutilsint

import (
	"context"
	"os"
	"testing"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/dinoctl"
	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var dinoclusterPath = func() string {
	envPath := os.Getenv("DINOCLUSTER_PATH")
	if envPath != "" {
		return envPath
	}

	return "cbdinocluster"
}()

func SkipIfNoDinoCluster(t *testing.T) {
	// skipping if no dinocluster implies skipping if short test
	SkipIfShortTest(t)

	if TestOpts.DinoClusterID == "" {
		t.Skip("skipping due to no dino cluster id")
	}
}

type DinoController struct {
	t             *testing.T
	dinoCtl       dinoctl.DinoCtl
	clusterId     string
	oldFoSettings *cbmgmtx.GetAutoFailoverSettingsResponse
	blockedNodes  []string
}

func StartDinoTesting(t *testing.T, disableAutoFailover bool) *DinoController {
	if TestOpts.DinoClusterID == "" {
		t.Error("cannot start dino testing without dino configured")
	}

	logger, _ := zap.NewDevelopment()
	c := &DinoController{
		t:         t,
		clusterId: TestOpts.DinoClusterID,
		dinoCtl: dinoctl.DinoCtl{
			Logger:    logger,
			LogOutput: true,
		},
	}
	t.Cleanup(c.cleanup)

	if disableAutoFailover {
		c.DisableAutoFailover()
	}

	return c
}

func (c *DinoController) cleanup() {
	blockedNodes := c.blockedNodes
	c.blockedNodes = nil

	for _, node := range blockedNodes {
		err := c.dinoCtl.ChaosAllowTraffic(c.clusterId, node)
		if err != nil {
			c.t.Errorf("failed to reset traffic control for %s", node)
		}
	}

	c.EnableAutoFailover()
}

func (c *DinoController) DisableAutoFailover() {
	settings, err := getTestMgmt().GetAutoFailoverSettings(context.Background(), &cbmgmtx.GetAutoFailoverSettingsRequest{})
	require.NoError(c.t, err)
	c.oldFoSettings = settings

	err = getTestMgmt().ConfigureAutoFailover(context.Background(), &cbmgmtx.ConfigureAutoFailoverRequest{
		Enabled: ptr.To(false),
	})
	require.NoError(c.t, err)
}

func (c *DinoController) EnableAutoFailover() {
	if c.oldFoSettings == nil {
		return
	}

	err := getTestMgmt().ConfigureAutoFailover(context.Background(), &cbmgmtx.ConfigureAutoFailoverRequest{
		Enabled: ptr.To(c.oldFoSettings.Enabled),
		Timeout: ptr.To(c.oldFoSettings.Timeout),
	})
	require.NoError(c.t, err)
	c.oldFoSettings = nil
}

func (c *DinoController) BlockNodeTraffic(node string) {
	c.blockedNodes = append(c.blockedNodes, node)
	err := c.dinoCtl.ChaosBlockTraffic(c.clusterId, node, "")
	require.NoError(c.t, err)
}

func (c *DinoController) BlockAllTraffic(node string) {
	c.blockedNodes = append(c.blockedNodes, node)
	err := c.dinoCtl.ChaosBlockTraffic(c.clusterId, node, "all")
	require.NoError(c.t, err)
}

func (c *DinoController) AllowTraffic(node string) {
	err := c.dinoCtl.ChaosAllowTraffic(c.clusterId, node)
	require.NoError(c.t, err)
	hostIdx := slices.Index(c.blockedNodes, node)
	if hostIdx >= 0 {
		c.blockedNodes = slices.Delete(c.blockedNodes, hostIdx, hostIdx)
	}
}
