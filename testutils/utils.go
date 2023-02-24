package testutils

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/couchbase/gocbcore/v10/connstr"
	"github.com/google/uuid"
	"golang.org/x/exp/slices"
)

var TestOpts TestOptions

type TestOptions struct {
	MemdAddrs         []string
	HTTPAddrs         []string
	LongTest          bool
	Username          string
	Password          string
	BucketName        string
	SupportedFeatures []TestFeature
	RunName           string
}

func addSupportedFeature(feat TestFeature) {
	if !slices.Contains(TestOpts.SupportedFeatures, feat) {
		TestOpts.SupportedFeatures = append(TestOpts.SupportedFeatures, feat)
	}
}
func removeSupportedFeature(feat TestFeature) {
	featIdx := slices.Index(TestOpts.SupportedFeatures, feat)
	if featIdx >= 0 {
		TestOpts.SupportedFeatures = slices.Delete(TestOpts.SupportedFeatures, featIdx, featIdx)
	}
}

func envFlagString(envName, name, value, usage string) *string {
	envValue := os.Getenv(envName)
	if envValue != "" {
		value = envValue
	}
	return flag.String(name, value, usage)
}

var connStr = envFlagString("GCBCONNSTR", "connstr", "",
	"Connection string to run tests with")
var user = envFlagString("GOCBUSER", "user", "",
	"The username to use to authenticate when using a real server")
var password = envFlagString("GOCBPASS", "pass", "",
	"The password to use to authenticate when using a real server")
var bucketName = envFlagString("GOCBBUCKET", "bucket", "default",
	"The bucket to use to test against")
var featsStr = envFlagString("GOCBFEAT", "features", "",
	"A comma-delimited list of features to test")

func SetupTests(m *testing.M) {
	flag.Parse()

	if *connStr != "" && !testing.Short() {
		TestOpts.LongTest = true
		err := parseConnStr(*connStr)
		if err != nil {
			panic("failed to parse connection string")
		}

		TestOpts.Username = *user
		if TestOpts.Username == "" {
			TestOpts.Username = "Administrator"
		}

		TestOpts.Password = *password
		if TestOpts.Password == "" {
			TestOpts.Password = "password"
		}

		if TestOpts.BucketName == "" {
			TestOpts.BucketName = *bucketName
		}
		if TestOpts.BucketName == "" {
			TestOpts.BucketName = "default"
		}
	}

	// default supported features
	TestOpts.SupportedFeatures = []TestFeature{}

	if featsStr != nil {
		featStrs := strings.Split(*featsStr, ",")
		for _, featStr := range featStrs {
			featStr = strings.TrimSpace(featStr)
			feat := TestFeature(strings.TrimLeft(featStr, "+-*"))

			if featStr == "*" {
				for _, feat := range AllTestFeatures {
					addSupportedFeature(feat)
				}
			} else if strings.HasPrefix(featStr, "-") {
				removeSupportedFeature(feat)
			} else {
				addSupportedFeature(feat)
			}
		}
	}

	TestOpts.RunName = strings.ReplaceAll(uuid.NewString(), "-", "")[0:8]

	result := m.Run()
	os.Exit(result)
}

// This is a bit of a halfway house whilst we evolve the API.
func parseConnStr(connStr string) error {
	baseSpec, err := connstr.Parse(connStr)
	if err != nil {
		return err
	}

	spec, err := connstr.Resolve(baseSpec)
	if err != nil {
		return err
	}

	var httpHosts []string
	for _, specHost := range spec.HttpHosts {
		httpHosts = append(httpHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
	}

	var memdHosts []string
	for _, specHost := range spec.MemdHosts {
		memdHosts = append(memdHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
	}

	TestOpts.MemdAddrs = memdHosts
	TestOpts.HTTPAddrs = httpHosts

	if spec.Bucket != "" {
		TestOpts.BucketName = spec.Bucket
	}

	return nil
}

func SkipIfShortTest(t *testing.T) {
	if !TestOpts.LongTest {
		t.Skipf("skipping long test")
	}
}
