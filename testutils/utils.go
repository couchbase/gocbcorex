package testutils

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

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
	OriginalConnStr   string
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

func SetupTests(m *testing.M) {
	initialGoroutineCount := runtime.NumGoroutine()
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
		TestOpts.OriginalConnStr = *connStr
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

	// We need to close the transport used by the default client once tests complete, otherwise the transport
	// will leak go routines.
	http.DefaultClient.CloseIdleConnections()
	// Loop for at most a second checking for goroutines leaks, this gives any HTTP goroutines time to shutdown
	start := time.Now()
	var finalGoroutineCount int
	for time.Since(start) <= 1*time.Second {
		runtime.Gosched()
		finalGoroutineCount = runtime.NumGoroutine()
		if finalGoroutineCount == initialGoroutineCount {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if finalGoroutineCount != initialGoroutineCount {
		log.Printf("Detected a goroutine leak (%d before != %d after)", initialGoroutineCount, finalGoroutineCount)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		result = 1
	} else {
		log.Printf("No goroutines appear to have leaked (%d before == %d after)", initialGoroutineCount, finalGoroutineCount)
	}

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

func FudgeConnStringToTLS(t *testing.T, connStr string) ([]string, []string) {
	split := strings.Split(connStr, "://")
	var baseConnStr string
	if len(split) == 1 {
		baseConnStr = split[0]
	} else {
		baseConnStr = split[1]
	}
	baseConnStr = "couchbases://" + baseConnStr

	baseSpec, err := connstr.Parse(baseConnStr)
	require.NoError(t, err)

	spec, err := connstr.Resolve(baseSpec)
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
