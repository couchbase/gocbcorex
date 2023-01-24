package testutils

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/couchbase/gocbcore/v10/connstr"
)

var TestOpts TestOptions

type TestOptions struct {
	MemdAddrs []string
	HTTPAddrs []string
	LongTest  bool
}

func SetupTests(m *testing.M) {
	connStr := envFlagString("GCBCONNSTR", "connstr", "",
		"Connection string to run tests with")
	flag.Parse()

	if *connStr != "" && !testing.Short() {
		TestOpts.LongTest = true
		err := parseConnStr(*connStr)
		if err != nil {
			panic("failed to parse connection string")
		}
	}

	result := m.Run()
	os.Exit(result)
}

func envFlagString(envName, name, value, usage string) *string {
	envValue := os.Getenv(envName)
	if envValue != "" {
		value = envValue
	}
	return flag.String(name, value, usage)
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

	return nil
}
