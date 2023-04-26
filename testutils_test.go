package gocbcorex

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/stretchr/testify/require"
)

func CreateAndEnsureScope(ctx context.Context, t *testing.T, agent *Agent, bucketName, scopeName string) {
	res, err := agent.CreateScope(context.Background(), &cbmgmtx.CreateScopeOptions{
		BucketName: bucketName,
		ScopeName:  scopeName,
	})
	require.NoError(t, err)

	uid, err := strconv.ParseUint(strings.Replace(res.ManifestUid, "0x", "", -1), 16, 64)
	require.NoError(t, err)

	WaitForManifest(ctx, t, agent, uid, bucketName)
}

func CreateAndEnsureCollection(ctx context.Context, t *testing.T, agent *Agent, bucketName, scopeName, collectionName string) {
	res, err := agent.CreateCollection(context.Background(), &cbmgmtx.CreateCollectionOptions{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	})
	require.NoError(t, err)

	uid, err := strconv.ParseUint(strings.Replace(res.ManifestUid, "0x", "", -1), 16, 64)
	require.NoError(t, err)

	WaitForManifest(ctx, t, agent, uid, bucketName)
}

func WaitForManifest(ctx context.Context, t *testing.T, agent *Agent, manifestID uint64, bucketName string) {
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

		uid, err := strconv.ParseUint(strings.Replace(manifest.UID, "0x", "", -1), 16, 64)
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
