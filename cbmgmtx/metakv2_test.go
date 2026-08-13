package cbmgmtx

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testRoundTripperFunc func(req *http.Request) (*http.Response, error)

func (f testRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNormalizeMetaKv2Path(t *testing.T) {
	assert.Equal(t, "/foo/bar", normalizeMetaKv2Path("foo/bar"))
	assert.Equal(t, "/foo/bar/", normalizeMetaKv2Path("/foo/bar/"))
	assert.Equal(t, "/", normalizeMetaKv2Path("/"))
}

func TestDecodeMetaKv2Error(t *testing.T) {
	type testCase struct {
		Name       string
		StatusCode int
		Body       string
		Expected   error
	}

	// servers older than 8.0.0 have no handler for the metakv2 paths, so they reply
	// with one of these generic non-json errors depending on the server version and
	// the method used.
	unsupportedCases := []testCase{
		{"UnsupportedGet", 404, "Not found.", ErrUnsupportedFeature},
		{"UnsupportedPut", 404, "Object Not Found", ErrUnsupportedFeature},
		{"UnsupportedPost", 404, "Requested resource not found.\n", ErrUnsupportedFeature},
		{"UnsupportedPut728", 405, "Method Not Allowed", ErrUnsupportedFeature},
		{"UnsupportedDelete728", 405, "Method Not Allowed", ErrUnsupportedFeature},
	}

	// servers which support metakv2 always reply with a structured json error.
	supportedCases := []testCase{
		{"NotFound", 404, `{"message":"Not Found","path":"/foo"}`, ErrMetaKvEntryNotFound},
		{"Conflict", 409, `{"message":"Conflict","path":"/foo","revision":"a:1"}`, ErrMetaKvConflict},
		{"NotEmpty", 400, `{"message":"Not Empty","path":"/foo/"}`, ErrMetaKvNotEmpty},
	}

	for _, tc := range append(unsupportedCases, supportedCases...) {
		t.Run(tc.Name, func(tt *testing.T) {
			err := decodeMetaKv2Error(&http.Response{
				StatusCode: tc.StatusCode,
				Body:       io.NopCloser(strings.NewReader(tc.Body)),
			})
			assert.ErrorIs(tt, err, tc.Expected)
		})
	}
}

func TestParseMetaKv2Directory(t *testing.T) {
	jsonPayload := `{
		"/root/subdir/": {
			"revision": "dir-rev-1",
			"value": {
				"/root/subdir/key1": {
					"revision": "rev-key-1",
					"value": "val1"
				},
				"/root/subdir/nested/": {
					"revision": "dir-rev-2",
					"value": {
						"/root/subdir/nested/key2": {
							"revision": "rev-key-2",
							"value": "val2"
						}
					}
				}
			}
		}
	}`

	entries := make(map[string]MetaKv2Entry)
	err := parseMetaKv2Directory(json.RawMessage(jsonPayload), entries)
	require.NoError(t, err)

	assert.Len(t, entries, 4)

	assert.True(t, entries["/root/subdir/"].IsDir)
	assert.Equal(t, "dir-rev-1", entries["/root/subdir/"].Revision)

	assert.False(t, entries["/root/subdir/key1"].IsDir)
	assert.Equal(t, "rev-key-1", entries["/root/subdir/key1"].Revision)
	assert.Equal(t, []byte("val1"), entries["/root/subdir/key1"].Value)

	assert.True(t, entries["/root/subdir/nested/"].IsDir)
	assert.Equal(t, "dir-rev-2", entries["/root/subdir/nested/"].Revision)

	assert.False(t, entries["/root/subdir/nested/key2"].IsDir)
	assert.Equal(t, "rev-key-2", entries["/root/subdir/nested/key2"].Revision)
	assert.Equal(t, []byte("val2"), entries["/root/subdir/nested/key2"].Value)
}

func TestSyncMetaKv2QuorumRequest(t *testing.T) {
	type testCase struct {
		Name        string
		Opts        *SyncMetaKv2QuorumOptions
		ExpectedUri string
		ExpectedObo string
	}

	testCases := []testCase{
		{
			Name:        "NoTimeout",
			Opts:        &SyncMetaKv2QuorumOptions{},
			ExpectedUri: "/_metakv2/_controller/syncQuorum",
		},
		{
			Name:        "WithTimeout",
			Opts:        &SyncMetaKv2QuorumOptions{Timeout: 5 * time.Second},
			ExpectedUri: "/_metakv2/_controller/syncQuorum?timeout=5000",
		},
		{
			Name: "WithOnBehalfOf",
			Opts: &SyncMetaKv2QuorumOptions{
				OnBehalfOf: &cbhttpx.OnBehalfOfInfo{Username: "user", Domain: "local"},
			},
			ExpectedUri: "/_metakv2/_controller/syncQuorum",
			ExpectedObo: "dXNlcjpsb2NhbA==",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(tt *testing.T) {
			var seenReq *http.Request
			mgmt := Management{
				Transport: testRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
					seenReq = req
					return &http.Response{
						StatusCode: 200,
						Body:       io.NopCloser(strings.NewReader("")),
					}, nil
				}),
				Endpoint: "http://localhost:8091",
			}

			err := mgmt.SyncMetaKv2Quorum(context.Background(), tc.Opts)
			require.NoError(tt, err)

			require.NotNil(tt, seenReq)
			assert.Equal(tt, "POST", seenReq.Method)
			assert.Equal(tt, tc.ExpectedUri, seenReq.URL.RequestURI())
			assert.Equal(tt, tc.ExpectedObo, seenReq.Header.Get("cb-on-behalf-of"))
		})
	}
}
