package gocbcorex_test

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/stretchr/testify/require"
)

func CreateAndEnsureScope(ctx context.Context, t *testing.T, agent *gocbcorex.Agent, bucketName, scopeName string) {
	res, err := agent.CreateScope(context.Background(), &cbmgmtx.CreateScopeOptions{
		BucketName: bucketName,
		ScopeName:  scopeName,
	})
	require.NoError(t, err)

	uid, err := strconv.ParseUint(strings.ReplaceAll(res.ManifestUid, "0x", ""), 16, 64)
	require.NoError(t, err)

	WaitForManifest(ctx, t, agent, uid, bucketName)
}

func CreateAndEnsureCollection(ctx context.Context, t *testing.T, agent *gocbcorex.Agent, bucketName, scopeName, collectionName string) {
	res, err := agent.CreateCollection(context.Background(), &cbmgmtx.CreateCollectionOptions{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	})
	require.NoError(t, err)

	uid, err := strconv.ParseUint(strings.ReplaceAll(res.ManifestUid, "0x", ""), 16, 64)
	require.NoError(t, err)

	WaitForManifest(ctx, t, agent, uid, bucketName)
}

func WaitForManifest(ctx context.Context, t *testing.T, agent *gocbcorex.Agent, manifestID uint64, bucketName string) {
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatalf("WaitForManifest requires deadlined context")
	}

	require.Eventually(t, func() bool {
		manifest, err := agent.GetCollectionManifest(ctx, &cbmgmtx.GetCollectionManifestOptions{
			BucketName: bucketName,
		})
		if err != nil {
			t.Logf("Failed to fetch manifest: %s", err)
			return false
		}

		uid, err := strconv.ParseUint(strings.ReplaceAll(manifest.UID, "0x", ""), 16, 64)
		if err != nil {
			t.Logf("Failed to parse uid: %s", err)
			return false
		}

		if uid < manifestID {
			t.Logf("Manifest uid too old, wanted %d but was %d", manifestID, uid)
			return false
		}

		return true
	}, time.Until(deadline), 100*time.Millisecond)
}

type ForwardingHttpRoundTripper struct {
	reqs        []*http.Request
	baseTripper *http.Transport
	interceptor func(req *http.Response)
}

func NewForwardingHttpRoundTripper(interceptor func(req *http.Response)) *ForwardingHttpRoundTripper {
	return &ForwardingHttpRoundTripper{
		baseTripper: &http.Transport{},
		interceptor: interceptor,
	}
}

func (rt *ForwardingHttpRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.reqs = append(rt.reqs, req)
	resp, err := rt.baseTripper.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if rt.interceptor != nil {
		rt.interceptor(resp)
	}

	return resp, nil
}

func (rt *ForwardingHttpRoundTripper) NumReqs() int {
	return len(rt.reqs)
}

func (rt *ForwardingHttpRoundTripper) Reqs() []*http.Request {
	return rt.reqs
}

func (rt *ForwardingHttpRoundTripper) Close() {
	rt.baseTripper.CloseIdleConnections()
}
