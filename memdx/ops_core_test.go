package memdx

import (
	"github.com/stretchr/testify/assert"
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

// Testing private functions isn't ideal but by far the way to ensure that our error handling does
// what is expected for all cases.
func TestOpsCoreDecodeError(t *testing.T) {
	type test struct {
		Name          string
		Pkt           *Packet
		ExpectedError error
	}

	dispatchedTo := "endpoint1"
	dispatchedFrom := "local1"

	tests := []test{
		{
			Name: "NotMyVbucket",
			Pkt: &Packet{
				Magic:  MagicResExt,
				OpCode: OpCodeReplace,
				Status: StatusNotMyVBucket,
				Opaque: 0x34,
				Value:  []byte("impretendingtobeaconfig"),
			},
			ExpectedError: ServerErrorWithConfig{
				Cause: ServerError{
					Cause:          ErrNotMyVbucket,
					DispatchedTo:   dispatchedTo,
					DispatchedFrom: dispatchedFrom,
					Opaque:         0x34,
				},
				ConfigJson: []byte("impretendingtobeaconfig"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			err := OpsCore{}.decodeError(test.Pkt, dispatchedTo, dispatchedFrom)

			assert.Equal(t, test.ExpectedError, err)
		})
	}
}
