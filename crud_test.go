package gocbcorex

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/gocbcorex/memdx"
)

func TestSimpleCrudCollectionMapOutdatedRetries(t *testing.T) {
	rs := NewRetryManagerDefault()
	var ourManifestRev uint64 = 5
	var ourSecondManifestRev uint64 = 6
	var ourCid uint32 = 9
	var ourSecondCid uint32 = 10
	firstServerManifestRev := 4
	var collectionCalls int
	cr := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			if collectionCalls == 0 {
				collectionCalls++
				return ourCid, ourManifestRev, nil
			}
			collectionCalls++

			return ourSecondCid, ourSecondManifestRev, nil
		},
		InvalidateCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string, endpoint string, manifestRev uint64) {
		},
	}
	vb := &VbucketRouterMock{
		DispatchByKeyFunc: func(key []byte, replicaID uint32) (string, uint16, error) {
			return "endpoint", 1, nil
		},
	}
	nkcp := &KvClientManagerMock{
		GetClientFunc: func(ctx context.Context, endpoint string) (KvClient, error) {
			return &KvClientMock{}, nil
		},
	}

	var fnCalls int
	fn := func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*UpsertResult, error) {
		if fnCalls == 0 {
			fnCalls++
			return nil, memdx.ServerErrorWithContext{
				Cause:       memdx.ServerError{Cause: memdx.ErrUnknownCollectionID},
				ContextJson: []byte(fmt.Sprintf(`{"manifest_uid":"%d"}`, firstServerManifestRev)),
			}
		}
		fnCalls++

		return &UpsertResult{}, nil
	}

	res, err := OrchestrateSimpleCrud[*UpsertResult](
		context.Background(),
		rs,
		cr,
		vb,
		nil,
		nkcp,
		"scope",
		"collection",
		[]byte("somekey"),
		fn,
	)
	require.NoError(t, err)
	require.NotNil(t, res)

	assert.Equal(t, 2, collectionCalls)
	assert.Equal(t, 2, fnCalls)
}
