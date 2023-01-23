package core

import (
	"context"
	"errors"

	"github.com/couchbase/stellar-nebula/core/memdx"
)

func OrchestrateMemdRetries[RespT any](
	ctx context.Context,
	rs RetryComponent,
	fn func() (RespT, error),
) (RespT, error) {
	for {
		res, err := fn()
		if err != nil {
			// TODO(brett19): Implement this
			/*
				retryTime := rs.GetRetryAction(err)
				if retryTime > 0 {
					err := contextSleep(ctx, retryTime)
					if err != nil {
						var emptyResp RespT
						return emptyResp, err
					}

					continue
				}
			*/

			return res, err
		}

		return res, nil
	}
}

func OrchestrateMemdCollectionID[RespT any](
	ctx context.Context,
	cr CollectionResolver,
	scopeName, collectionName string,
	fn func(collectionID uint32, manifestID uint64) (RespT, error),
) (RespT, error) {
	collectionID, manifestID, err := cr.ResolveCollectionID(ctx, "", scopeName, collectionName)
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

func OrchestrateMemdRouting[RespT any](
	ctx context.Context,
	vd VbucketDispatcher,
	key []byte,
	fn func(endpoint string, vbID uint16) (RespT, error),
) (RespT, error) {
	for {
		endpoint, vbID, err := vd.DispatchByKey(ctx, key)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		// Implement me properly
		res, err := fn(endpoint, vbID)
		if err != nil {
			if errors.Is(err, memdx.ErrNotMyVbucket) {
				// TODO: this will need to retry or something.
			}

			return res, err
		}

		return res, nil
	}
}

func OrchestrateMemdClient[RespT any](
	ctx context.Context,
	cm EndpointConnectionProvider,
	endpoint string,
	fn func(client KvClient) (RespT, error),
) (RespT, error) {
	for {
		cli, err := cm.GetClient(ctx, endpoint)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		res, err := fn(cli)
		if err != nil {
			return res, err
		}

		return res, nil
	}
}

func OrchestrateSimpleCrud[RespT any](
	ctx context.Context,
	rs RetryComponent,
	cr CollectionResolver,
	vb VbucketDispatcher,
	cm EndpointConnectionProvider,
	scopeName, collectionName string,
	key []byte,
	fn func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (RespT, error),
) (RespT, error) {
	return OrchestrateMemdRetries(
		ctx, rs,
		func() (RespT, error) {
			return OrchestrateMemdCollectionID(
				ctx, cr, scopeName, collectionName,
				func(collectionID uint32, manifestID uint64) (RespT, error) {
					return OrchestrateMemdRouting(
						ctx, vb, key,
						func(endpoint string, vbID uint16) (RespT, error) {
							return OrchestrateMemdClient(ctx, cm, endpoint, func(client KvClient) (RespT, error) {
								return fn(collectionID, manifestID, endpoint, vbID, client)
							})
						})

				})
		})
}
