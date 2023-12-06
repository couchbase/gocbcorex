package testutils

import (
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func SkipIfNoDinoCluster(t *testing.T) {
	// skipping if no dinocluster implies skipping if short test
	SkipIfShortTest(t)

	if TestOpts.DinoClusterID == "" {
		t.Skip("skipping due to no dino cluster id")
	}
}

var dinoclusterPath = func() string {
	envPath := os.Getenv("DINOCLUSTER_PATH")
	if envPath != "" {
		return envPath
	}

	return "cbdinocluster"
}()

func RunDinoCmd(t *testing.T, args []string) {
	cmd := exec.Command(dinoclusterPath, append([]string{"-v"}, args...)...)
	log.Printf("running command: %s ", strings.Join(cmd.Args, " "))
	log.Printf("---")

	stdOut, _ := cmd.StdoutPipe()
	stdErr, _ := cmd.StderrPipe()
	go func() { _, _ = io.Copy(os.Stdout, stdOut) }()
	go func() { _, _ = io.Copy(os.Stdout, stdErr) }()

	err := cmd.Run()

	log.Printf("---")

	require.NoError(t, err)
}

func DinoBlockTraffic(t *testing.T, node string) {
	RunDinoCmd(t, []string{"chaos", "block-traffic", TestOpts.DinoClusterID, node})
}

func DinoAllowTraffic(t *testing.T, node string) {
	RunDinoCmd(t, []string{"chaos", "allow-traffic", TestOpts.DinoClusterID, node})
}
