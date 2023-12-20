package cbqueryx_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbqueryx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testRoundTripper struct {
	ReceivedRequests []*http.Request
	Responses        []unifiedResponseError

	count int
}

func (rt *testRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.ReceivedRequests = append(rt.ReceivedRequests, req)

	c := rt.count
	rt.count++
	return rt.Responses[c].Response, rt.Responses[c].Err
}

func makeSingleTestRoundTripper(resp *http.Response, err error) *testRoundTripper {
	return &testRoundTripper{
		Responses: []unifiedResponseError{
			{
				Response: resp,
				Err:      err,
			},
		},
	}
}

type queryErrorJson struct {
	Code   uint32                 `json:"code,omitempty"`
	Msg    string                 `json:"msg,omitempty"`
	Reason map[string]interface{} `json:"reason,omitempty"`
	Retry  bool                   `json:"retry,omitempty"`
}

// Note that the ordering of this struct matters, it needs to match the query JSON ordering.
type testQueryResult struct {
	RequestID       string `json:"requestID"`
	ClientContextID string `json:"clientContextID"`
	Signature       struct {
		I        string `json:"i"`
		TestName string `json:"testName"`
	} `json:"signature"`
	Metrics struct {
		ElapsedTime   string `json:"elapsedTime"`
		ExecutionTime string `json:"executionTime"`
		ResultCount   uint64 `json:"resultCount"`
		ResultSize    uint64 `json:"resultSize"`
		ServiceLoad   int    `json:"serviceLoad"`
		ErrorCount    int    `json:"errorCount"`
	} `json:"metrics"`
	Prepared string           `json:"prepared,omitempty"`
	Results  json.RawMessage  `json:"results"`
	Errors   []queryErrorJson `json:"errors,omitempty"`
	Status   string           `json:"status"`
}

func assertQueryResult(t *testing.T, expectedRows []string, expectedResult *testQueryResult, res cbqueryx.ResultStream) {
	var rows [][]byte
	for {
		row, err := res.ReadRow()
		require.NoError(t, err)

		if row == nil {
			break
		}

		rows = append(rows, row)
	}

	assert.Len(t, rows, len(expectedRows))
	for i, row := range rows {
		assert.Equal(t, []byte(expectedRows[i]), row)
	}

	meta, err := res.MetaData()
	require.NoError(t, err)

	assert.Equal(t, cbqueryx.Status(expectedResult.Status), meta.Status)
	assert.Equal(t, expectedResult.ClientContextID, meta.ClientContextID)
	assert.Equal(t, expectedResult.RequestID, meta.RequestID)

	elapsedTime, err := time.ParseDuration(expectedResult.Metrics.ElapsedTime)
	require.NoError(t, err)

	executionTime, err := time.ParseDuration(expectedResult.Metrics.ExecutionTime)
	require.NoError(t, err)

	assert.Equal(t, elapsedTime, meta.Metrics.ElapsedTime)
	assert.Equal(t, executionTime, meta.Metrics.ExecutionTime)
	assert.Equal(t, expectedResult.Metrics.ResultCount, meta.Metrics.ResultCount)
	assert.Equal(t, expectedResult.Metrics.ResultSize, meta.Metrics.ResultSize)
}

func makeSuccessQueryResult(rows []string, prepared string) testQueryResult {
	qr := testQueryResult{
		Results:         json.RawMessage("[" + strings.Join(rows, ",") + "]"),
		RequestID:       "941e33f4-15d8-44d0-a5a8-422e3e84039f",
		ClientContextID: "12345",
		Signature: struct {
			I        string `json:"i"`
			TestName string `json:"testName"`
		}{
			I:        "json",
			TestName: "json",
		},
		Status: "success",
		Metrics: struct {
			ElapsedTime   string `json:"elapsedTime"`
			ExecutionTime string `json:"executionTime"`
			ResultCount   uint64 `json:"resultCount"`
			ResultSize    uint64 `json:"resultSize"`
			ServiceLoad   int    `json:"serviceLoad"`
			ErrorCount    int    `json:"errorCount"`
		}{
			ElapsedTime:   "129.569043ms",
			ExecutionTime: "129.521515ms",
			ResultCount:   2,
			ResultSize:    20,
			ServiceLoad:   2,
		},
	}
	if prepared != "" {
		qr.Prepared = prepared
	}

	return qr
}

func makeErrorQueryResult(errors []queryErrorJson) testQueryResult {
	qr := testQueryResult{
		Results:         json.RawMessage("[]"),
		RequestID:       "941e33f4-15d8-44d0-a5a8-422e3e84039f",
		ClientContextID: "12345",
		Status:          "errors",
		Metrics: struct {
			ElapsedTime   string `json:"elapsedTime"`
			ExecutionTime string `json:"executionTime"`
			ResultCount   uint64 `json:"resultCount"`
			ResultSize    uint64 `json:"resultSize"`
			ServiceLoad   int    `json:"serviceLoad"`
			ErrorCount    int    `json:"errorCount"`
		}{
			ElapsedTime:   "129.569043ms",
			ExecutionTime: "129.521515ms",
			ResultCount:   0,
			ResultSize:    0,
			ServiceLoad:   2,
			ErrorCount:    1,
		},
		Errors: errors,
	}

	return qr
}

type unifiedResponseError struct {
	Response *http.Response
	Err      error
}
