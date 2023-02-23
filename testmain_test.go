package gocbcorex

import (
	"os"
	"testing"

	"github.com/couchbase/gocbcorex/testutils"
)

func TestMain(m *testing.M) {
	testutils.SetupTests(m)
}

func LoadTestData(t *testing.T, path string) []byte {
	s, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err.Error())
	}

	return s
}
