package cbqueryx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/couchbase/gocbcorex/cbqueryx"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	cache := cbqueryx.NewPreparedStatementCache()
	opts := &cbqueryx.Options{
		Statement: "SELECT 1",
	}
	rt := makeSingleTestRoundTripper(resp, nil)
	res, err := cbqueryx.PreparedQuery{
		Executor: &cbqueryx.Query{
			Transport: rt,
			Logger:    testutils.MakeTestLogger(t),
			UserAgent: "useragent",
			Username:  "username",
			Password:  "password",
		},
		Cache: cache,
	}.PreparedQuery(context.Background(), opts)
	require.NoError(t, err)

	assertQueryResult(t, expectedRows, &expectedResult, res)

	if assert.Len(t, rt.ReceivedRequests, 1) {
		req := rt.ReceivedRequests[0]
		body, err := io.ReadAll(req.Body)
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
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	opts := &cbqueryx.Options{
		Statement: "SELECT 1",
	}
	cache := cbqueryx.NewPreparedStatementCache()
	cache.Put(opts.Statement, "apreparedstatement")
	rt := makeSingleTestRoundTripper(resp, nil)
	res, err := cbqueryx.PreparedQuery{
		Executor: &cbqueryx.Query{
			Transport: rt,
			Logger:    testutils.MakeTestLogger(t),
			UserAgent: "useragent",
			Username:  "username",
			Password:  "password",
		},
		Cache: cache,
	}.PreparedQuery(context.Background(), opts)
	require.NoError(t, err)

	assertQueryResult(t, expectedRows, &expectedResult, res)

	if assert.Len(t, rt.ReceivedRequests, 1) {
		req := rt.ReceivedRequests[0]
		body, err := io.ReadAll(req.Body)
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
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	opts := &cbqueryx.Options{
		Statement: "SELECT 1",
	}
	cache := cbqueryx.NewPreparedStatementCache()
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
	res, err := cbqueryx.PreparedQuery{
		Executor: &cbqueryx.Query{
			Transport: rt,
			Logger:    testutils.MakeTestLogger(t),
			UserAgent: "useragent",
			Username:  "username",
			Password:  "password",
		},
		Cache: cache,
	}.PreparedQuery(context.Background(), opts)
	require.NoError(t, err)

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
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	cache := cbqueryx.NewPreparedStatementCache()
	opts := &cbqueryx.Options{
		Statement: "SELECT 1",
	}
	rt := makeSingleTestRoundTripper(resp, nil)
	res, err := cbqueryx.PreparedQuery{
		Executor: &cbqueryx.Query{
			Transport: rt,
			Logger:    testutils.MakeTestLogger(t),
			UserAgent: "useragent",
			Username:  "username",
			Password:  "password",
		},
		Cache: cache,
	}.PreparedQuery(context.Background(), opts)
	require.NoError(t, err)

	assertQueryResult(t, expectedRows, &expectedResult, res)

	stmt, ok := cache.Get("SELECT 1=1")
	require.False(t, ok)
	require.Empty(t, stmt)
}
