package gocbcorex

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrchestrateMemdCollectionID(t *testing.T) {
	cid := uint32(5)
	rev := uint64(2)
	expectedScopeName := "testScope"
	expectedCollectionName := "testCol"
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			assert.Equal(t, expectedScopeName, scopeName)
			assert.Equal(t, expectedCollectionName, collectionName)

			return cid, rev, nil
		},
	}

	var called int
	ctx := context.Background()
	res, err := OrchestrateMemdCollectionID(ctx, mock, expectedScopeName, expectedCollectionName, func(collectionID uint32, manifestID uint64) (int, error) {
		called++

		assert.Equal(t, cid, collectionID)
		assert.Equal(t, rev, manifestID)

		return 1, nil
	})
	require.NoError(t, err)

	assert.Equal(t, 1, called)
	assert.Equal(t, 1, res)
}

func TestOrchestrateMemdCollectionIDReturnError(t *testing.T) {
	cid := uint32(5)
	rev := uint64(2)
	expectedScopeName := "testScope"
	expectedCollectionName := "testCol"
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			assert.Equal(t, expectedScopeName, scopeName)
			assert.Equal(t, expectedCollectionName, collectionName)

			return cid, rev, nil
		},
	}

	var called int
	expectedErr := errors.New("imanerror")
	ctx := context.Background()
	res, err := OrchestrateMemdCollectionID(ctx, mock, expectedScopeName, expectedCollectionName, func(collectionID uint32, manifestID uint64) (int, error) {
		called++

		assert.Equal(t, cid, collectionID)
		assert.Equal(t, rev, manifestID)

		return 0, expectedErr
	})
	require.ErrorIs(t, err, expectedErr)

	assert.Equal(t, 1, called)
	assert.Zero(t, res)
}

func TestOrchestrateMemdCollectionIDResolverReturnError(t *testing.T) {
	expectedScopeName := "testScope"
	expectedCollectionName := "testCol"
	expectedErr := errors.New("imanerror")

	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			assert.Equal(t, expectedScopeName, scopeName)
			assert.Equal(t, expectedCollectionName, collectionName)

			return 0, 0, expectedErr
		},
	}

	var called int
	ctx := context.Background()
	res, err := OrchestrateMemdCollectionID(ctx, mock, expectedScopeName, expectedCollectionName, func(collectionID uint32, manifestID uint64) (int, error) {
		called++

		return 0, errors.New("shouldnt have reached here")
	})
	require.ErrorIs(t, err, expectedErr)

	assert.Zero(t, called)
	assert.Zero(t, res)
}

func TestOrchestrateMemdCollectionIDCollectionNotFoundError(t *testing.T) {
	cid := uint32(5)
	rev := uint64(2)
	var numInvalidateCalls int
	expectedScopeName := "testScope"
	expectedCollectionName := "testCol"
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			assert.Equal(t, expectedScopeName, scopeName)
			assert.Equal(t, expectedCollectionName, collectionName)

			return cid, rev, nil
		},
		InvalidateCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string, endpoint string, manifestRev uint64) {
			assert.Equal(t, expectedScopeName, scopeName)
			assert.Equal(t, expectedCollectionName, collectionName)

			numInvalidateCalls++
		},
	}

	var called int
	ctx := context.Background()
	res, err := OrchestrateMemdCollectionID(ctx, mock, expectedScopeName, expectedCollectionName, func(collectionID uint32, manifestID uint64) (int, error) {
		called++

		assert.Equal(t, cid, collectionID)
		assert.Equal(t, rev, manifestID)

		return 0, memdx.ErrUnknownCollectionID
	})
	require.ErrorIs(t, err, ErrCollectionManifestOutdated)

	assert.Equal(t, 1, called)
	assert.Equal(t, 1, numInvalidateCalls)
	assert.Zero(t, res)
}

func TestOrchestrateMemdCollectionIDCollectionNotFoundErrorServerHasOlderManifest(t *testing.T) {
	cid := uint32(5)
	rev := uint64(2)
	var numInvalidateCalls int
	expectedScopeName := "testScope"
	expectedCollectionName := "testCol"
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			assert.Equal(t, expectedScopeName, scopeName)
			assert.Equal(t, expectedCollectionName, collectionName)

			return cid, rev, nil
		},
		InvalidateCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string, endpoint string, manifestRev uint64) {
			assert.Equal(t, expectedScopeName, scopeName)
			assert.Equal(t, expectedCollectionName, collectionName)

			numInvalidateCalls++
		},
	}

	errorContext := struct {
		Context     string `json:"context"`
		Ref         string `json:"ref"`
		ManifestUID string `json:"manifest_uid"`
	}{
		ManifestUID: "1",
	}

	contextBytes, err := json.Marshal(errorContext)
	require.NoError(t, err)

	var called int
	ctx := context.Background()
	res, err := OrchestrateMemdCollectionID(ctx, mock, expectedScopeName, expectedCollectionName, func(collectionID uint32, manifestID uint64) (int, error) {
		called++

		assert.Equal(t, cid, collectionID)
		assert.Equal(t, rev, manifestID)

		return 0, memdx.ServerErrorWithContext{
			Cause: memdx.ServerError{
				Cause: memdx.ErrUnknownCollectionID,
			},
			ContextJson: contextBytes,
		}
	})
	require.ErrorIs(t, err, ErrCollectionManifestOutdated)

	var errorT *CollectionManifestOutdatedError
	if assert.ErrorAs(t, err, &errorT) {
		assert.Equal(t, rev, errorT.ManifestUid)
		assert.Equal(t, uint64(1), errorT.ServerManifestUid)
	}

	assert.Equal(t, 1, called)
	assert.Equal(t, 0, numInvalidateCalls)
	assert.Zero(t, res)
}
