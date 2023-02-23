package cbhttpx

import (
	"context"
	"net/http"
	"testing"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/require"
)

func TestHttpMgmtTerseClusterConfig(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	resp, err := HttpManagement{
		HttpClient: &http.Client{},
		UserAgent:  "gocbcorex test",
		Endpoint:   "http://" + testutils.TestOpts.HTTPAddrs[0],
		Username:   testutils.TestOpts.Username,
		Password:   testutils.TestOpts.Password,
	}.GetTerseClusterConfig(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Greater(t, resp.Rev, 0)
}
