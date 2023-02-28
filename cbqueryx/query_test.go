package cbqueryx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/gocbcorex/testutils"
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
		Body:          ioutil.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	cache := NewPreparedStatementCache()
	opts := &QueryOptions{
		Statement: "SELECT 1",
		Endpoint:  "endpoint1",
	}
	res, err := Query{
		HttpClient: &http.Client{Transport: makeSingleTestRoundTripper(resp, nil)},
		Logger:     testutils.MakeTestLogger(t),
		QueryCache: cache,
		UserAgent:  "useragent",
		Username:   "username",
		Password:   "password",
	}.Query(context.Background(), opts)
	require.NoError(t, err)

	assert.Equal(t, opts.Endpoint, res.Endpoint())

	assertQueryResult(t, expectedRows, &expectedResult, res)

	require.Empty(t, cache.queryCache)
}

func TestPreparedQuery(t *testing.T) {
	expectedRows := []string{
		`{"test":"value"}`,
		`{"test2":"value2"}`,
	}
	expectedResult := makeSuccessQueryResult(expectedRows, "somepreparedstatement")
	body, err := json.Marshal(expectedResult)
	require.NoError(t, err)

	resp := &http.Response{
		Status:        "success",
		StatusCode:    200,
		Header:        nil,
		Body:          ioutil.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	cache := NewPreparedStatementCache()
	opts := &QueryOptions{
		Statement: "SELECT 1",
		Endpoint:  "endpoint1",
	}
	rt := makeSingleTestRoundTripper(resp, nil)
	res, err := Query{
		HttpClient: &http.Client{Transport: rt},
		Logger:     testutils.MakeTestLogger(t),
		QueryCache: cache,
		UserAgent:  "useragent",
		Username:   "username",
		Password:   "password",
	}.PreparedQuery(context.Background(), opts)
	require.NoError(t, err)

	assert.Equal(t, opts.Endpoint, res.Endpoint())

	assertQueryResult(t, expectedRows, &expectedResult, res)

	if assert.Len(t, rt.ReceivedRequests, 1) {
		req := rt.ReceivedRequests[0]
		body, err := ioutil.ReadAll(req.Body)
		require.NoError(t, err)

		var m map[string]interface{}
		require.NoError(t, json.Unmarshal(body, &m))

		assert.Equal(t, "PREPARE "+opts.Statement, m["statement"])
		assert.True(t, m["auto_execute"].(bool))
	}

	prepared, _ := cache.Get(opts.Statement)
	require.Equal(t, "somepreparedstatement", prepared)
}

func TestPreparedQueryAlreadyCached(t *testing.T) {
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
		Body:          ioutil.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	opts := &QueryOptions{
		Statement: "SELECT 1",
		Endpoint:  "endpoint1",
	}
	cache := NewPreparedStatementCache()
	cache.Put(opts.Statement, "apreparedstatement")
	rt := makeSingleTestRoundTripper(resp, nil)
	res, err := Query{
		HttpClient: &http.Client{Transport: rt},
		Logger:     testutils.MakeTestLogger(t),
		QueryCache: cache,
		UserAgent:  "useragent",
		Username:   "username",
		Password:   "password",
	}.PreparedQuery(context.Background(), opts)
	require.NoError(t, err)

	assert.Equal(t, opts.Endpoint, res.Endpoint())

	assertQueryResult(t, expectedRows, &expectedResult, res)

	if assert.Len(t, rt.ReceivedRequests, 1) {
		req := rt.ReceivedRequests[0]
		body, err := ioutil.ReadAll(req.Body)
		require.NoError(t, err)

		var m map[string]interface{}
		require.NoError(t, json.Unmarshal(body, &m))

		assert.Empty(t, m["statement"])
		assert.Equal(t, "apreparedstatement", m["prepared"])
	}

	prepared, _ := cache.Get(opts.Statement)
	require.Equal(t, "apreparedstatement", prepared)
}

func TestPreparedQueryAlreadyCachedVersionFails(t *testing.T) {
	expectedRows := []string{
		`{"test":"value"}`,
		`{"test2":"value2"}`,
	}
	expectedResult := makeSuccessQueryResult(expectedRows, "apreparedstatement")
	body, err := json.Marshal(expectedResult)
	require.NoError(t, err)

	resp := &http.Response{
		Status:        "success",
		StatusCode:    200,
		Header:        nil,
		Body:          ioutil.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	opts := &QueryOptions{
		Statement: "SELECT 1",
		Endpoint:  "endpoint1",
	}
	cache := NewPreparedStatementCache()
	cache.Put(opts.Statement, "apreparedstatement")
	rt := &testRoundTripper{
		Responses: []unifiedResponseError{
			{
				Err: errors.New("an error occurred"),
			},
			{
				Response: resp,
			},
		},
	}
	res, err := Query{
		HttpClient: &http.Client{Transport: rt},
		Logger:     testutils.MakeTestLogger(t),
		QueryCache: cache,
		UserAgent:  "useragent",
		Username:   "username",
		Password:   "password",
	}.PreparedQuery(context.Background(), opts)
	require.NoError(t, err)

	assert.Equal(t, opts.Endpoint, res.Endpoint())

	assertQueryResult(t, expectedRows, &expectedResult, res)

	assert.Len(t, rt.ReceivedRequests, 2)

	prepared, _ := cache.Get(opts.Statement)
	require.Equal(t, "apreparedstatement", prepared)
}

func TestPreparedQueryPreparedNameMissing(t *testing.T) {
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
		Body:          ioutil.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	cache := NewPreparedStatementCache()
	opts := &QueryOptions{
		Statement: "SELECT 1",
		Endpoint:  "endpoint1",
	}
	rt := makeSingleTestRoundTripper(resp, nil)
	res, err := Query{
		HttpClient: &http.Client{Transport: rt},
		Logger:     testutils.MakeTestLogger(t),
		QueryCache: cache,
		UserAgent:  "useragent",
		Username:   "username",
		Password:   "password",
	}.PreparedQuery(context.Background(), opts)
	require.NoError(t, err)

	assert.Equal(t, opts.Endpoint, res.Endpoint())

	assertQueryResult(t, expectedRows, &expectedResult, res)

	require.Empty(t, cache.queryCache)
}

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
	} `json:"metrics"`
	Prepared string          `json:"prepared,omitempty"`
	Results  json.RawMessage `json:"results"`
	Status   string          `json:"status"`
}

func assertQueryResult(t *testing.T, expectedRows []string, expectedResult *testQueryResult, res *QueryRowReader) {
	var rows [][]byte
	for {
		row := res.NextRow()
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

	assert.Equal(t, QueryStatus(expectedResult.Status), meta.Status)
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

	assert.NoError(t, res.Err())
	assert.NoError(t, res.Close())
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

type unifiedResponseError struct {
	Response *http.Response
	Err      error
}
