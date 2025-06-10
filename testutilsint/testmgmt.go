package testutilsint

import (
	"net/http"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
)

func getTestMgmt() cbmgmtx.Management {
	return cbmgmtx.Management{
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  "http://" + TestOpts.HTTPAddrs[0],
		Auth: &cbhttpx.BasicAuth{
			Username: TestOpts.Username,
			Password: TestOpts.Password,
		},
	}
}
