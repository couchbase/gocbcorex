package testutils

import (
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func MakeTestLogger(t *testing.T) *zap.Logger {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	return logger
}

func LoadTestData(t *testing.T, filename string) []byte {
	_, root, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(root), "..")
	err := os.Chdir(dir)
	require.NoError(t, err)

	b, err := os.ReadFile(dir + "/testdata/" + filename)
	require.NoError(t, err)

	return b
}
