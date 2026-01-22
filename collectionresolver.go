package gocbcorex

import (
	"context"
	"errors"

	"github.com/couchbase/gocbcorex/memdx"
)

type CollectionResolver interface {
	ResolveCollectionID(ctx context.Context, scopeName, collectionName string) (collectionId uint32, manifestRev uint64, err error)
	InvalidateCollectionID(ctx context.Context, scopeName, collectionName, endpoint string, manifestRev uint64)
}

type MemdCollectionIdOrchestrator struct {
	ctx            context.Context
	cr             CollectionResolver
	scopeName      string
	collectionName string
	collectionID   uint32

	resolvedCid uint32
	manifestRev uint64
}

func NewMemdCollectionIdOrchestrator(ctx context.Context, cr CollectionResolver, scopeName, collectionName string, collectionID uint32) (*MemdCollectionIdOrchestrator, error) {
	o := &MemdCollectionIdOrchestrator{
		ctx:            ctx,
		cr:             cr,
		scopeName:      scopeName,
		collectionName: collectionName,
		collectionID:   collectionID,
	}

	err := o.begin()
	if err != nil {
		return nil, err
	}

	return o, nil
}

func (o *MemdCollectionIdOrchestrator) begin() error {
	if o.collectionID > 0 && o.collectionName == "" && o.scopeName == "" {
		// If there's an unknown collection ID error then we'll just propagate it.
		o.resolvedCid = o.collectionID
		return nil
	}

	resolvedCid, manifestRev, err := o.cr.ResolveCollectionID(o.ctx, o.scopeName, o.collectionName)
	if err != nil {
		return err
	}

	o.resolvedCid = resolvedCid
	o.manifestRev = manifestRev

	if o.collectionID > 0 && resolvedCid != o.collectionID {
		o.cr.InvalidateCollectionID(
			o.ctx,
			o.scopeName, o.collectionName,
			"", 0)

		newCollectionID, newManifestRev, newResolveErr :=
			o.cr.ResolveCollectionID(o.ctx, o.scopeName, o.collectionName)
		if newResolveErr != nil {
			return newResolveErr
		}

		if newCollectionID != o.collectionID {
			// If we still don't match after resolution, then we can confidently say that we have the latest
			// so the callee must have an out of date collection ID.
			return &CollectionIDMismatchError{
				CollectionID:       o.collectionID,
				ServerCollectionID: newCollectionID,
				ManifestUid:        newManifestRev,
			}
		}
	}

	return nil
}

func (o *MemdCollectionIdOrchestrator) HandleError(err error) error {
	if errors.Is(err, memdx.ErrUnknownCollectionID) {
		invalidatingEndpoint := ""
		invalidatingManifestRev := uint64(0)

		var serverErr *memdx.ServerErrorWithContext
		if errors.As(err, &serverErr) {
			serverCtx := serverErr.ParseContext()
			invalidatingManifestRev = serverCtx.ManifestRev
		}

		if invalidatingManifestRev > 0 &&
			invalidatingManifestRev < o.manifestRev {
			return &CollectionManifestOutdatedError{
				Cause:             err,
				ManifestUid:       o.manifestRev,
				ServerManifestUid: invalidatingManifestRev,
			}
		}

		o.cr.InvalidateCollectionID(
			o.ctx,
			o.scopeName, o.collectionName,
			invalidatingEndpoint, invalidatingManifestRev)

		newCollectionID, newManifestRev, newResolveErr :=
			o.cr.ResolveCollectionID(o.ctx, o.scopeName, o.collectionName)
		if newResolveErr != nil {
			return newResolveErr
		}

		if newCollectionID == o.resolvedCid {
			// if resolution yielded the same response, this means that our ability
			// to fetch an updated collection id is compromised, or the server is in
			// an older state.  In both instances, we no longer have a deterministic
			// path to resolution and return the error, allowing retries to occur
			// at a higher level if desired.
			return &CollectionManifestOutdatedError{
				Cause:             err,
				ManifestUid:       o.manifestRev,
				ServerManifestUid: invalidatingManifestRev,
			}
		}

		o.resolvedCid = newCollectionID
		o.manifestRev = newManifestRev
		return nil
	}

	return err
}

func (o *MemdCollectionIdOrchestrator) GetCollectionID() uint32 {
	return o.resolvedCid
}

func OrchestrateMemdCollectionID[RespT any](
	ctx context.Context,
	cr CollectionResolver,
	scopeName, collectionName string,
	collectionID uint32,
	fn func(collectionID uint32) (RespT, error),
) (RespT, error) {
	if collectionID > 0 && collectionName == "" && scopeName == "" {
		// If there's an unknown collection ID error then we'll just propagate it.
		return fn(collectionID)
	}

	resolvedCid, manifestRev, err := cr.ResolveCollectionID(ctx, scopeName, collectionName)
	if err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	if collectionID > 0 && resolvedCid != collectionID {
		cr.InvalidateCollectionID(
			ctx,
			scopeName, collectionName,
			"", 0)

		newCollectionID, newManifestRev, newResolveErr :=
			cr.ResolveCollectionID(ctx, scopeName, collectionName)
		if newResolveErr != nil {
			var emptyResp RespT
			return emptyResp, newResolveErr
		}

		if newCollectionID != collectionID {
			// If we still don't match after resolution, then we can confidently say that we have the latest
			// so the callee must have an out of date collection ID.
			var emptyResp RespT
			return emptyResp, &CollectionIDMismatchError{
				CollectionID:       collectionID,
				ServerCollectionID: newCollectionID,
				ManifestUid:        newManifestRev,
			}
		}
	}

	for {
		res, err := fn(resolvedCid)
		if err != nil {
			if errors.Is(err, memdx.ErrUnknownCollectionID) {
				invalidatingEndpoint := ""
				invalidatingManifestRev := uint64(0)

				var serverErr *memdx.ServerErrorWithContext
				if errors.As(err, &serverErr) {
					serverCtx := serverErr.ParseContext()
					invalidatingManifestRev = serverCtx.ManifestRev
				}

				if invalidatingManifestRev > 0 &&
					invalidatingManifestRev < manifestRev {
					var emptyResp RespT
					return emptyResp, &CollectionManifestOutdatedError{
						Cause:             err,
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

				if newCollectionID == resolvedCid {
					// if resolution yielded the same response, this means that our ability
					// to fetch an updated collection id is compromised, or the server is in
					// an older state.  In both instances, we no longer have a deterministic
					// path to resolution and return the error, allowing retries to occur
					// at a higher level if desired.
					var emptyResp RespT
					return emptyResp, &CollectionManifestOutdatedError{
						Cause:             err,
						ManifestUid:       manifestRev,
						ServerManifestUid: invalidatingManifestRev,
					}
				}

				resolvedCid = newCollectionID
				manifestRev = newManifestRev
				continue
			}

			return res, err
		}

		return res, nil
	}
}
