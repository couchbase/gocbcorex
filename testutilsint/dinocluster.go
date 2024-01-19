package testutilsint

import (
	"context"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/stretchr/testify/require"
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

func runDinoCmd(args []string) error {
	cmd := exec.Command(dinoclusterPath, append([]string{"-v"}, args...)...)
	log.Printf("running command: %s ", strings.Join(cmd.Args, " "))
	log.Printf("---")

	stdOut, _ := cmd.StdoutPipe()
	stdErr, _ := cmd.StderrPipe()
	go func() { _, _ = io.Copy(os.Stdout, stdOut) }()
	go func() { _, _ = io.Copy(os.Stdout, stdErr) }()

	err := cmd.Run()

	log.Printf("---")

	return err
}

func runDinoBlockTraffic(node string) error {
	return runDinoCmd([]string{"chaos", "block-traffic", TestOpts.DinoClusterID, node})
}

func runDinoAllowTraffic(node string) error {
	return runDinoCmd([]string{"chaos", "allow-traffic", TestOpts.DinoClusterID, node})
}

func RunGenericDinoCmd(t *testing.T, args []string) {
	err := runDinoCmd(args)
	require.NoError(t, err)
}

type DinoController struct {
	t             *testing.T
	oldFoSettings *cbmgmtx.GetAutoFailoverSettingsResponse
	blockedNodes  []string
}

func StartDinoTesting(t *testing.T, disableAutoFailover bool) *DinoController {
	if TestOpts.DinoClusterID == "" {
		t.Error("cannot start dino testing without dino configured")
	}

	c := &DinoController{t: t}
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
		err := runDinoAllowTraffic(node)
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

func (c *DinoController) BlockTraffic(node string) {
	c.blockedNodes = append(c.blockedNodes, node)
	err := runDinoBlockTraffic(node)
	require.NoError(c.t, err)
}

func (c *DinoController) AllowTraffic(node string) {
	err := runDinoAllowTraffic(node)
	require.NoError(c.t, err)
	hostIdx := slices.Index(c.blockedNodes, node)
	if hostIdx >= 0 {
		c.blockedNodes = slices.Delete(c.blockedNodes, hostIdx, hostIdx)
	}
}
