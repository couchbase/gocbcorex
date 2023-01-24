package core

import (
	"context"

	"github.com/couchbase/stellar-nebula/core/memdx"
)

type CollectionResolverMemd struct {
	connMgr EndpointConnectionProvider
}

var _ CollectionResolver = (*CollectionResolverMemd)(nil)

func (cr *CollectionResolverMemd) ResolveCollectionID(
	ctx context.Context, scopeName, collectionName string,
) (collectionId uint32, manifestRev uint64, err error) {
	resp, err := OrchestrateRandomMemdClient(
		ctx, cr.connMgr,
		func(client KvClient) (*memdx.GetCollectionIDResponse, error) {
			return client.GetCollectionID(ctx, &memdx.GetCollectionIDRequest{
				ScopeName:      scopeName,
				CollectionName: collectionName,
			})
		})
	if err != nil {
		return 0, 0, err
	}

	return resp.CollectionID, resp.ManifestID, nil
}

func (cr *CollectionResolverMemd) InvalidateCollectionID(
	ctx context.Context, scopeName, collectionName, endpoint string, manifestRev uint64,
) {
	// Every collection resolution request yields a new operation to the server
	// meaning every resolution request is 'guarenteed' to be up to date.
}
