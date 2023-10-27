package cbqueryx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"

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
	opts := &Options{
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

func TestQueryIndexExists(t *testing.T) {
	index := uuid.NewString()[:6]
	expectedResult := makeErrorQueryResult([]queryErrorJson{
		{
			Code: 4300,
			Msg:  fmt.Sprintf("The index %s already exists.", index),
			Reason: map[string]interface{}{
				"name": index,
			},
		},
	})
	body, err := json.Marshal(expectedResult)
	require.NoError(t, err)

	resp := &http.Response{
		Status:        "conflict",
		StatusCode:    409,
		Header:        nil,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}

	opts := &QueryOptions{
		Statement: fmt.Sprintf("CREATE INDEX %s", index),
	}
	_, err = Query{
		Transport: makeSingleTestRoundTripper(resp, nil),
		Logger:    testutils.MakeTestLogger(t),
		UserAgent: "useragent",
		Username:  "username",
		Password:  "password",
	}.Query(context.Background(), opts)
	assert.ErrorIs(t, err, ErrIndexExists)
}

func TestQueryIndexNotFound(t *testing.T) {
	index := uuid.NewString()[:6]
	expectedResult := makeErrorQueryResult([]queryErrorJson{
		{
			Code: 12016,
			Msg:  fmt.Sprintf("Index Not Found - cause: GSI index %s not found.", index),
			Reason: map[string]interface{}{
				"name": index,
			},
		},
	})
	body, err := json.Marshal(expectedResult)
	require.NoError(t, err)

	resp := &http.Response{
		Status:        "success",
		StatusCode:    200,
		Header:        nil,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}

	opts := &QueryOptions{
		Statement: fmt.Sprintf("CREATE INDEX %s", index),
	}
	_, err = Query{
		Transport: makeSingleTestRoundTripper(resp, nil),
		Logger:    testutils.MakeTestLogger(t),
		UserAgent: "useragent",
		Username:  "username",
		Password:  "password",
	}.Query(context.Background(), opts)
	assert.ErrorIs(t, err, ErrIndexNotFound)
}
