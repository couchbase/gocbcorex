package memdx_test

import (
	"testing"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"

	"github.com/stretchr/testify/require"
)

func TestOpsCoreGetCollectionIDBasic(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	// we intentionally request a non-default collection to guarentee that
	// collection-id and manifest-rev are non-zero.
	resp, err := memdx.SyncUnaryCall(memdx.OpsUtils{
		ExtFramesEnabled: false,
	}, memdx.OpsUtils.GetCollectionID, cli, &memdx.GetCollectionIDRequest{
		ScopeName:      "_default",
		CollectionName: "test",
	})
	require.NoError(t, err)
	require.NotEqual(t, uint32(0), resp.CollectionID)
	require.NotEqual(t, uint64(0), resp.ManifestRev)
}

func TestOpsCoreGetCollectionIDCollectionMissing(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	_, err := memdx.SyncUnaryCall(memdx.OpsUtils{
		ExtFramesEnabled: false,
	}, memdx.OpsUtils.GetCollectionID, cli, &memdx.GetCollectionIDRequest{
		ScopeName:      "_default",
		CollectionName: "invalid-collection",
	})
	require.ErrorIs(t, err, memdx.ErrUnknownCollectionName)

	var serverErr memdx.ServerErrorWithContext
	require.ErrorAs(t, err, &serverErr)
	serverCtx := serverErr.ParseContext()

	require.NotEqual(t, 0, serverCtx.ManifestRev)
}

func TestOpsCoreGetCollectionIDScopeMissing(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	_, err := memdx.SyncUnaryCall(memdx.OpsUtils{
		ExtFramesEnabled: false,
	}, memdx.OpsUtils.GetCollectionID, cli, &memdx.GetCollectionIDRequest{
		ScopeName:      "invalid-scope",
		CollectionName: "_default",
	})
	require.ErrorIs(t, err, memdx.ErrUnknownScopeName)

	if testutilsint.IsServerVersionBetween(t, "7.6.0", "7.6.3") {
		// BUG(ING-949): MB-64026 caused context to be missing in these server versions.
	} else {
		var serverErr memdx.ServerErrorWithContext
		require.ErrorAs(t, err, &serverErr)
		serverCtx := serverErr.ParseContext()

		require.NotEqual(t, 0, serverCtx.ManifestRev)
	}
}
