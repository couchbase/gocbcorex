package dinoctl

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var defaultDinoPath = func() string {
	envPath := os.Getenv("DINOCLUSTER_PATH")
	if envPath != "" {
		return envPath
	}

	return "cbdinocluster"
}()

type DinoCtl struct {
	Logger    *zap.Logger
	LogOutput bool
	Path      string
}

func (c DinoCtl) ExecPath() string {
	if c.Path != "" {
		return c.Path
	}

	return defaultDinoPath
}

type AllocDef struct {
	SimpleName string
	Def        string
	DefFile    string
}

func (def AllocDef) toArgs() ([]string, error) {
	if def.SimpleName != "" {
		return []string{def.SimpleName}, nil
	} else if def.Def != "" {
		return []string{"--def", string(def.Def)}, nil
	} else if def.DefFile != "" {
		return []string{"--def-file", def.DefFile}, nil
	}

	return nil, errors.New("invalid cluster definition")
}

func (c DinoCtl) Alloc(def AllocDef) (string, error) {
	defArgs, err := def.toArgs()
	if err != nil {
		return "", err
	}

	return c.Exec(append([]string{"alloc"}, defArgs...))
}

func (c DinoCtl) Modify(clusterId string, def AllocDef) error {
	defArgs, err := def.toArgs()
	if err != nil {
		return err
	}

	_, err = c.Exec(append([]string{"modify", clusterId}, defArgs...))
	return err
}

func (c DinoCtl) Remove(clusterId string) error {
	_, err := c.Exec([]string{"rm", clusterId})
	return err
}

func (c DinoCtl) Connstr(clusterId string) (string, error) {
	return c.Exec([]string{"connstr", clusterId})
}

func (c DinoCtl) Mgmt(clusterId string) (string, error) {
	return c.Exec([]string{"mgmt", clusterId})
}

func (c DinoCtl) Ip(clusterId string) (string, error) {
	return c.Exec([]string{"ip", clusterId})
}

func (c DinoCtl) ChaosBlockTraffic(clusterId, nodeId, trafficType string) error {
	if trafficType == "" {
		_, err := c.Exec([]string{"chaos", "block-traffic", clusterId, nodeId})
		return err
	}

	_, err := c.Exec([]string{"chaos", "block-traffic", clusterId, nodeId, trafficType})
	return err
}

func (c DinoCtl) ChaosAllowTraffic(clusterId, nodeId string) error {
	_, err := c.Exec([]string{"chaos", "allow-traffic", clusterId, nodeId})
	return err
}

func (c DinoCtl) Exec(args []string) (string, error) {
	c.Logger.Debug("running dinoctl command",
		zap.String("execPath", c.ExecPath()),
		zap.Strings("args", args))

	if c.Logger.Core().Enabled(zapcore.DebugLevel) {
		args = append([]string{"-v"}, args...)
	}

	cmd := exec.Command(c.ExecPath(), args...)

	stdErr, _ := cmd.StderrPipe()
	stdOut, _ := cmd.StdoutPipe()
	stdOutBuffer := bytes.NewBuffer(nil)
	stdOutTee := io.TeeReader(stdOut, stdOutBuffer)

	shouldLogOutput := c.LogOutput
	if shouldLogOutput {
		argsText := ""
		for _, arg := range args {
			arg := strings.ReplaceAll(arg, "\n", "\\n")
			if argsText != "" {
				argsText += " "
			}
			if len(arg) > 50 {
				argsText += "'" + arg[:50] + "'..."
			} else {
				argsText += "'" + arg + "'"
			}
		}

		fmt.Printf("---- dino exec output (exec path: %s, args: %s)\n",
			c.ExecPath(),
			argsText)

		go func() {
			scanner := bufio.NewScanner(stdErr)
			for scanner.Scan() {
				fmt.Printf("dino-err: %s\n", scanner.Text())
			}
		}()

		go func() {
			scanner := bufio.NewScanner(stdOutTee)
			for scanner.Scan() {
				fmt.Printf("dino-out: %s\n", scanner.Text())
			}
		}()
	}

	err := cmd.Run()

	if shouldLogOutput {
		fmt.Printf("---- dino exec completed\n")
	}

	scanner := bufio.NewScanner(stdOutBuffer)
	stdOutText := ""
	for scanner.Scan() {
		if stdOutText != "" {
			stdOutText = stdOutText + "\n"
		}
		stdOutText += scanner.Text()
	}

	c.Logger.Debug("exec output", zap.String("result", stdOutText))

	return stdOutText, err
}
