package memdx

import (
	"testing"

	"github.com/couchbase/gocbcorex/testutils"

	"github.com/stretchr/testify/require"
)

func TestOpsCoreGetCollectionIDBasic(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	cli := createTestClient(t)

	// we intentionally request a non-default collection to guarentee that
	// collection-id and manifest-rev are non-zero.
	resp, err := syncUnaryCall(OpsUtils{
		ExtFramesEnabled: false,
	}, OpsUtils.GetCollectionID, cli, &GetCollectionIDRequest{
		ScopeName:      "_default",
		CollectionName: "test",
	})
	require.NoError(t, err)
	require.NotEqual(t, uint32(0), resp.CollectionID)
	require.NotEqual(t, uint64(0), resp.ManifestRev)
}

func TestOpsCoreGetCollectionIDCollectionMissing(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	cli := createTestClient(t)

	_, err := syncUnaryCall(OpsUtils{
		ExtFramesEnabled: false,
	}, OpsUtils.GetCollectionID, cli, &GetCollectionIDRequest{
		ScopeName:      "_default",
		CollectionName: "invalid-collection",
	})
	require.ErrorIs(t, err, ErrUnknownCollectionName)

	var serverErr ServerErrorWithContext
	require.ErrorAs(t, err, &serverErr)
	serverCtx := serverErr.ParseContext()

	require.NotEqual(t, 0, serverCtx.ManifestRev)
}

func TestOpsCoreGetCollectionIDScopeMissing(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	cli := createTestClient(t)

	_, err := syncUnaryCall(OpsUtils{
		ExtFramesEnabled: false,
	}, OpsUtils.GetCollectionID, cli, &GetCollectionIDRequest{
		ScopeName:      "invalid-scope",
		CollectionName: "_default",
	})
	require.ErrorIs(t, err, ErrUnknownScopeName)

	var serverErr ServerErrorWithContext
	require.ErrorAs(t, err, &serverErr)
	serverCtx := serverErr.ParseContext()

	require.NotEqual(t, 0, serverCtx.ManifestRev)
}
