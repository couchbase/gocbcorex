package memdx

import (
	"github.com/couchbase/stellar-nebula/core/testutils"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpsCoreGetCollectionIDBasic(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	cli := createTestClient(t)

	resp, err := syncUnaryCall(OpsUtils{
		ExtFramesEnabled: false,
	}, OpsUtils.GetCollectionID, cli, &GetCollectionIDRequest{
		ScopeName:      "_default",
		CollectionName: "_default",
	})
	require.NoError(t, err)
	require.Equal(t, uint32(0), resp.CollectionID)
	require.Equal(t, uint64(0), resp.ManifestID)
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

	var serverErr ServerError
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

	var serverErr ServerError
	require.ErrorAs(t, err, &serverErr)
	serverCtx := serverErr.ParseContext()

	require.NotEqual(t, 0, serverCtx.ManifestRev)
}
