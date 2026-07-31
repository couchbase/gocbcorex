package cbmgmtx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// supportsMetaKv2 identifies whether the server we are testing against exposes the
// metakv2 endpoints, which were only introduced in server 8.0.0.  Older servers have
// no handler registered for these paths and instead report them as unsupported.
func supportsMetaKv2(t *testing.T) bool {
	return !testutilsint.IsOlderServerVersion(t, "8.0.0")
}

func TestMetaKv2Leaves(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()
	mgmt := getHttpMgmt()

	rootPath := "/test-root-" + uuid.NewString()[:6]
	keyPath := rootPath + "/subdir/key1"

	if !supportsMetaKv2(t) {
		_, err := mgmt.GetMetaKv2(ctx, &cbmgmtx.GetMetaKv2Options{
			Path: keyPath,
		})
		require.ErrorIs(t, err, cbmgmtx.ErrUnsupportedFeature)

		_, err = mgmt.PutMetaKv2(ctx, &cbmgmtx.PutMetaKv2Options{
			Path:      keyPath,
			Value:     []byte("value1"),
			Create:    true,
			Recursive: true,
		})
		require.ErrorIs(t, err, cbmgmtx.ErrUnsupportedFeature)

		_, err = mgmt.DeleteMetaKv2(ctx, &cbmgmtx.DeleteMetaKv2Options{
			Path: keyPath,
		})
		require.ErrorIs(t, err, cbmgmtx.ErrUnsupportedFeature)
		return
	}

	// 1. Get non-existent key -> ErrMetaKvEntryNotFound
	getResp, err := mgmt.GetMetaKv2(ctx, &cbmgmtx.GetMetaKv2Options{
		Path: keyPath,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, cbmgmtx.ErrMetaKvEntryNotFound))
	require.Nil(t, getResp)

	// 2. Put key without recursive -> should fail because parent dirs don't exist
	putResp, err := mgmt.PutMetaKv2(ctx, &cbmgmtx.PutMetaKv2Options{
		Path:   keyPath,
		Value:  []byte("value1"),
		Create: true,
	})
	require.Error(t, err)
	require.Nil(t, putResp)

	// 3. Put key with recursive -> success
	putResp, err = mgmt.PutMetaKv2(ctx, &cbmgmtx.PutMetaKv2Options{
		Path:      keyPath,
		Value:     []byte("value1"),
		Create:    true,
		Recursive: true,
	})
	require.NoError(t, err)
	require.NotNil(t, putResp)
	require.NotEmpty(t, putResp.Revision)

	initialRev := putResp.Revision

	// 4. Get key -> verify value and revision
	getResp, err = mgmt.GetMetaKv2(ctx, &cbmgmtx.GetMetaKv2Options{
		Path: keyPath,
	})
	require.NoError(t, err)
	require.NotNil(t, getResp)
	assert.Equal(t, []byte("value1"), getResp.Value)
	assert.Equal(t, initialRev, getResp.Revision)

	// 5. Put with incorrect revision (CAS conflict) -> ErrMetaKvConflict
	putResp, err = mgmt.PutMetaKv2(ctx, &cbmgmtx.PutMetaKv2Options{
		Path:     keyPath,
		Value:    []byte("value2"),
		Revision: "fake:999",
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, cbmgmtx.ErrMetaKvConflict))
	require.Nil(t, putResp)

	// 6. Put with correct revision -> success
	putResp, err = mgmt.PutMetaKv2(ctx, &cbmgmtx.PutMetaKv2Options{
		Path:     keyPath,
		Value:    []byte("value2"),
		Revision: initialRev,
	})
	require.NoError(t, err)
	require.NotNil(t, putResp)
	assert.NotEqual(t, initialRev, putResp.Revision)

	// Cleanup root directory recursively
	_, _ = mgmt.DeleteMetaKv2(ctx, &cbmgmtx.DeleteMetaKv2Options{
		Path:      rootPath + "/",
		Recursive: true,
	})
}

func TestMetaKv2Directories(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()
	mgmt := getHttpMgmt()

	rootPath := "/test-dir-" + uuid.NewString()[:6]
	dirPath := rootPath + "/subdir/"

	if !supportsMetaKv2(t) {
		_, err := mgmt.PutMetaKv2(ctx, &cbmgmtx.PutMetaKv2Options{
			Path:      dirPath,
			Recursive: true,
		})
		require.ErrorIs(t, err, cbmgmtx.ErrUnsupportedFeature)

		_, err = mgmt.GetMetaKv2(ctx, &cbmgmtx.GetMetaKv2Options{
			Path:      rootPath + "/",
			Recursive: true,
		})
		require.ErrorIs(t, err, cbmgmtx.ErrUnsupportedFeature)

		_, err = mgmt.DeleteMetaKv2(ctx, &cbmgmtx.DeleteMetaKv2Options{
			Path:      rootPath + "/",
			Recursive: true,
		})
		require.ErrorIs(t, err, cbmgmtx.ErrUnsupportedFeature)
		return
	}

	// 1. Create directory recursively
	putResp, err := mgmt.PutMetaKv2(ctx, &cbmgmtx.PutMetaKv2Options{
		Path:      dirPath,
		Recursive: true,
	})
	require.NoError(t, err)
	require.NotNil(t, putResp)

	// 2. Put leaf keys in directory
	k1 := dirPath + "key1"
	k2 := dirPath + "nested/key2"

	_, err = mgmt.PutMetaKv2(ctx, &cbmgmtx.PutMetaKv2Options{
		Path:      k1,
		Value:     []byte("v1"),
		Create:    true,
		Recursive: true,
	})
	require.NoError(t, err)

	_, err = mgmt.PutMetaKv2(ctx, &cbmgmtx.PutMetaKv2Options{
		Path:      k2,
		Value:     []byte("v2"),
		Create:    true,
		Recursive: true,
	})
	require.NoError(t, err)

	// 3. Get directory recursively
	getResp, err := mgmt.GetMetaKv2(ctx, &cbmgmtx.GetMetaKv2Options{
		Path:      rootPath + "/",
		Recursive: true,
	})
	require.NoError(t, err)
	require.NotNil(t, getResp)
	require.NotEmpty(t, getResp.Entries)

	assert.Contains(t, getResp.Entries, k1)
	assert.Equal(t, []byte("v1"), getResp.Entries[k1].Value)

	assert.Contains(t, getResp.Entries, k2)
	assert.Equal(t, []byte("v2"), getResp.Entries[k2].Value)

	// 4. Delete non-empty directory non-recursively -> ErrMetaKvNotEmpty
	_, err = mgmt.DeleteMetaKv2(ctx, &cbmgmtx.DeleteMetaKv2Options{
		Path:      dirPath,
		Recursive: false,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, cbmgmtx.ErrMetaKvNotEmpty))

	// 5. Delete directory recursively -> success
	delResp, err := mgmt.DeleteMetaKv2(ctx, &cbmgmtx.DeleteMetaKv2Options{
		Path:      rootPath + "/",
		Recursive: true,
	})
	require.NoError(t, err)
	require.NotNil(t, delResp)
}

func TestMetaKv2Snapshot(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()
	mgmt := getHttpMgmt()

	rootPath := "/test-snap-" + uuid.NewString()[:6]
	k1 := rootPath + "/k1"
	k2 := rootPath + "/k2"

	if !supportsMetaKv2(t) {
		_, err := mgmt.GetMetaKv2Snapshot(ctx, &cbmgmtx.GetMetaKv2SnapshotOptions{
			Keys: []string{k1, k2},
		})
		require.ErrorIs(t, err, cbmgmtx.ErrUnsupportedFeature)
		return
	}

	_, err := mgmt.PutMetaKv2(ctx, &cbmgmtx.PutMetaKv2Options{
		Path:      k1,
		Value:     []byte("snap1"),
		Create:    true,
		Recursive: true,
	})
	require.NoError(t, err)

	_, err = mgmt.PutMetaKv2(ctx, &cbmgmtx.PutMetaKv2Options{
		Path:      k2,
		Value:     []byte("snap2"),
		Create:    true,
		Recursive: true,
	})
	require.NoError(t, err)

	snapResp, err := mgmt.GetMetaKv2Snapshot(ctx, &cbmgmtx.GetMetaKv2SnapshotOptions{
		Keys: []string{k1, k2},
	})
	require.NoError(t, err)
	require.NotNil(t, snapResp)
	assert.Len(t, snapResp.Entries, 2)
	assert.Equal(t, []byte("snap1"), snapResp.Entries[k1].Value)
	assert.Equal(t, []byte("snap2"), snapResp.Entries[k2].Value)

	// Cleanup
	_, _ = mgmt.DeleteMetaKv2(ctx, &cbmgmtx.DeleteMetaKv2Options{
		Path:      rootPath + "/",
		Recursive: true,
	})
}

func TestMetaKv2SetMultiple(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()
	mgmt := getHttpMgmt()

	rootPath := "/test-mult-" + uuid.NewString()[:6]
	k1 := rootPath + "/k1"
	k2 := rootPath + "/subdir/k2"

	setResp, err := mgmt.SetMetaKv2Multiple(ctx, &cbmgmtx.SetMetaKv2MultipleOptions{
		Entries: map[string]cbmgmtx.MetaKv2SetEntry{
			k1: {Value: "m1", Create: true},
			k2: {Value: "m2", Create: true},
		},
		Recursive: true,
	})

	if !supportsMetaKv2(t) {
		require.ErrorIs(t, err, cbmgmtx.ErrUnsupportedFeature)
		return
	}

	require.NoError(t, err)
	require.NotNil(t, setResp)

	// Verify entries were set
	g1, err := mgmt.GetMetaKv2(ctx, &cbmgmtx.GetMetaKv2Options{Path: k1})
	require.NoError(t, err)
	assert.Equal(t, []byte("m1"), g1.Value)

	g2, err := mgmt.GetMetaKv2(ctx, &cbmgmtx.GetMetaKv2Options{Path: k2})
	require.NoError(t, err)
	assert.Equal(t, []byte("m2"), g2.Value)

	// Cleanup
	_, _ = mgmt.DeleteMetaKv2(ctx, &cbmgmtx.DeleteMetaKv2Options{
		Path:      rootPath + "/",
		Recursive: true,
	})
}

func TestMetaKv2SyncQuorum(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()
	mgmt := getHttpMgmt()

	err := mgmt.SyncMetaKv2Quorum(ctx, &cbmgmtx.SyncMetaKv2QuorumOptions{
		Timeout: 5 * time.Second,
	})

	if !supportsMetaKv2(t) {
		require.ErrorIs(t, err, cbmgmtx.ErrUnsupportedFeature)
		return
	}

	require.NoError(t, err)
}

func TestMetaKvWatchHelper(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgmt := getHttpMgmt()

	watchDir := "/test-watch-hlpr-" + uuid.NewString()[:6] + "/"
	keyPath := watchDir + "key1"

	hlpr := cbmgmtx.MetaKvWatchHelper{
		Path:         watchDir,
		PollInterval: 100 * time.Millisecond,
	}

	ch, err := hlpr.Watch(ctx, &cbmgmtx.MetaKvWatchOptions{
		Management: *mgmt,
	})

	if !supportsMetaKv2(t) {
		// the watcher polls using GetMetaKv2, so it cannot even be started against a
		// server which does not support metakv2.
		require.ErrorIs(t, err, cbmgmtx.ErrUnsupportedFeature)
		require.Nil(t, ch)
		return
	}

	require.NoError(t, err)

	// 1. Initial result signal should be received immediately
	select {
	case <-ch:
		// First result received
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for initial watch signal")
	}

	// 2. Put a key to trigger a revision change
	putResp, err := mgmt.PutMetaKv2(context.Background(), &cbmgmtx.PutMetaKv2Options{
		Path:      keyPath,
		Value:     []byte("watchval"),
		Create:    true,
		Recursive: true,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, putResp.Revision)

	// 3. Expect second signal on channel for revision change
	select {
	case <-ch:
		// Revision change signal received
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watch revision change signal")
	}

	// Cleanup key
	_, _ = mgmt.DeleteMetaKv2(context.Background(), &cbmgmtx.DeleteMetaKv2Options{
		Path:      watchDir,
		Recursive: true,
	})
}
