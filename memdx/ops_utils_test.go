package memdx_test

import (
	"log"
	"testing"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
)

func TestOpsUtilsStats(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	opsUtils := memdx.OpsUtils{
		ExtFramesEnabled: false,
	}

	waitCh := make(chan error, 10)

	opsUtils.Stats(cli, &memdx.StatsRequest{
		GroupName: "vbucket-details 1",
	}, func(resp *memdx.StatsDataResponse) {
		log.Printf("Data: %v", resp)
	}, func(resp *memdx.StatsActionResponse, err error) {
		log.Printf("Resp: %v, Error: %v", resp, err)
		waitCh <- err
	})
	err := <-waitCh
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
