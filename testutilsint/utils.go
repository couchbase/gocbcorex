package testutilsint

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbaselabs/gocbconnstr/v2"

	"github.com/couchbase/gocbcorex/contrib/leakcheck"
	"golang.org/x/exp/slices"
	"golang.org/x/mod/semver"
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
	OriginalConnStr   string
	ClusterVersion    string
	DinoClusterID     string
}

func addSupportedFeature(feat TestFeature) {
	if !slices.Contains(TestOpts.SupportedFeatures, feat) {
		TestOpts.SupportedFeatures = append(TestOpts.SupportedFeatures, feat)
	}
}
func removeSupportedFeature(feat TestFeature) {
	featIdx := slices.Index(TestOpts.SupportedFeatures, feat)
	if featIdx >= 0 {
		TestOpts.SupportedFeatures = slices.Delete(TestOpts.SupportedFeatures, featIdx, featIdx+1)
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
var dinoId = envFlagString("GCBDINOID", "dinoid", "",
	"Dinocluster cluster id")

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

		TestOpts.DinoClusterID = *dinoId

		if TestOpts.BucketName == "" {
			TestOpts.BucketName = *bucketName
		}
		if TestOpts.BucketName == "" {
			TestOpts.BucketName = "default"
		}
		TestOpts.OriginalConnStr = *connStr

		if len(TestOpts.HTTPAddrs) == 0 {
			panic("could not identify http hosts for version lookup")
		}

		firstHost := TestOpts.HTTPAddrs[0]

		req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s/pools", firstHost), nil)
		req.SetBasicAuth(TestOpts.Username, TestOpts.Password)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("failed to get cluster version: %s", err)
		} else {
			respBytes, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				log.Printf("failed to get cluster version: %s", err)
			} else {
				var respSt struct {
					ImplementationVersion string `json:"implementationVersion"`
				}
				err = json.Unmarshal(respBytes, &respSt)
				if err != nil {
					log.Printf("failed to get cluster version: %s", err)
				} else {
					TestOpts.ClusterVersion = strings.Split(respSt.ImplementationVersion, "-")[0]
				}
			}
		}
		log.Printf("identified cluster version is: %s", TestOpts.ClusterVersion)
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

	leakcheck.EnableAll()

	result := m.Run()

	// We need to close the transport used by the default client once tests
	// complete, otherwise the transport will leak go routines.
	http.DefaultClient.CloseIdleConnections()

	if !leakcheck.ReportAll() {
		result = 1
	}

	os.Exit(result)
}

// This is a bit of a halfway house whilst we evolve the API.
func parseConnStr(connStr string) error {
	baseSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return err
	}

	spec, err := gocbconnstr.Resolve(baseSpec)
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

func SkipIfOlderServerVersion(t *testing.T, version string) {
	if semver.Compare("v"+TestOpts.ClusterVersion, "v"+version) < 0 {
		t.Skipf("skipping test for mismatched server version, %s < %s",
			TestOpts.ClusterVersion, version)
	}
}

func FudgeConnStringToTLS(t *testing.T, connStr string) ([]string, []string) {
	split := strings.Split(connStr, "://")
	var baseConnStr string
	if len(split) == 1 {
		baseConnStr = split[0]
	} else {
		baseConnStr = split[1]
	}
	baseConnStr = "couchbases://" + baseConnStr

	baseSpec, err := gocbconnstr.Parse(baseConnStr)
	require.NoError(t, err)

	spec, err := gocbconnstr.Resolve(baseSpec)
	require.NoError(t, err)

	var httpHosts []string
	for _, specHost := range spec.HttpHosts {
		httpHosts = append(httpHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
	}

	var memdHosts []string
	for _, specHost := range spec.MemdHosts {
		memdHosts = append(memdHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
	}

	return memdHosts, httpHosts
}
