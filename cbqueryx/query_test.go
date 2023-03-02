package cbqueryx

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	expectedRows := []string{
		`{"test":"value"}`,
		`{"test2":"value2"}`,
	}
	expectedResult := makeSuccessQueryResult(expectedRows, "")
	body, err := json.Marshal(expectedResult)
	require.NoError(t, err)

	resp := &http.Response{
		Status:        "success",
		StatusCode:    200,
		Header:        nil,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	cache := NewPreparedStatementCache()
	opts := &QueryOptions{
		Statement: "SELECT 1",
	}
	res, err := Query{
		Transport: makeSingleTestRoundTripper(resp, nil),
		Logger:    testutils.MakeTestLogger(t),
		UserAgent: "useragent",
		Username:  "username",
		Password:  "password",
	}.Query(context.Background(), opts)
	require.NoError(t, err)

	assertQueryResult(t, expectedRows, &expectedResult, res)

	require.Empty(t, cache.queryCache)
}
