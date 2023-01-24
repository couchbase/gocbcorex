package core

import (
	"context"
	"errors"

	"github.com/couchbase/stellar-nebula/core/memdx"
)

type CollectionResolver interface {
	ResolveCollectionID(ctx context.Context, scopeName, collectionName string) (collectionId uint32, manifestRev uint64, err error)
	InvalidateCollectionID(ctx context.Context, scopeName, collectionName, endpoint string, manifestRev uint64)
}

func OrchestrateMemdCollectionID[RespT any](
	ctx context.Context,
	cr CollectionResolver,
	scopeName, collectionName string,
	fn func(collectionID uint32, manifestID uint64) (RespT, error),
) (RespT, error) {
	collectionID, manifestID, err := cr.ResolveCollectionID(ctx, scopeName, collectionName)
	if err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	for {
		res, err := fn(collectionID, manifestID)
		if err != nil {
			if errors.Is(err, memdx.ErrUnknownCollectionID) {
				// TODO(brett19): Implement the endpoint and collectionID info here...
				cr.InvalidateCollectionID(ctx, scopeName, collectionName, "", 0)

				// TODO(brett19): Implement actually retrying when possible.
				// Note that we need to return the error if our second resolution yields
				// the same information we already had rather than looping.  In case the
				// server is "perpetually behind".
			}

			return res, err
		}

		return res, nil
	}
}
