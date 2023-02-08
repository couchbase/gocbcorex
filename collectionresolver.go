package core

import (
	"context"
	"errors"

	"github.com/couchbase/gocbcorex/memdx"
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
	collectionID, manifestRev, err := cr.ResolveCollectionID(ctx, scopeName, collectionName)
	if err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	for {
		res, err := fn(collectionID, manifestRev)
		if err != nil {
			if errors.Is(err, memdx.ErrUnknownCollectionID) {
				invalidatingEndpoint := ""
				invalidatingManifestRev := uint64(0)

				var serverErr memdx.ServerErrorWithContext
				if errors.As(err, &serverErr) {
					serverCtx := serverErr.ParseContext()
					invalidatingManifestRev = serverCtx.ManifestRev
				}

				if invalidatingManifestRev > 0 &&
					invalidatingManifestRev < manifestRev {
					var emptyResp RespT
					return emptyResp, ServerManifestOutdatedError{
						ManifestUid:       manifestRev,
						ServerManifestUid: invalidatingManifestRev,
					}
				}

				cr.InvalidateCollectionID(
					ctx,
					scopeName, collectionName,
					invalidatingEndpoint, invalidatingManifestRev)

				newCollectionID, newManifestRev, newResolveErr :=
					cr.ResolveCollectionID(ctx, scopeName, collectionName)
				if newResolveErr != nil {
					var emptyResp RespT
					return emptyResp, newResolveErr
				}

				if newCollectionID == collectionID {
					// if resolution yielded the same response, this means that our ability
					// to fetch an updated collection id is compromised, or the server is in
					// an older state.  In both instances, we no longer have a deterministic
					// path to resolution and return the error, allowing retries to occur
					// at a higher level if desired.
					var emptyResp RespT
					return emptyResp, err
				}

				collectionID = newCollectionID
				manifestRev = newManifestRev
				continue
			}

			return res, err
		}

		return res, nil
	}
}
