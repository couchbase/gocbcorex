package gocbcorex

import (
	"context"

	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

type CollectionResolverMemdOptions struct {
	Logger         *zap.Logger
	ClientProvider KvClientProvider
}

type CollectionResolverMemd struct {
	logger         *zap.Logger
	clientProvider KvClientProvider
}

var _ CollectionResolver = (*CollectionResolverMemd)(nil)

func NewCollectionResolverMemd(opts *CollectionResolverMemdOptions) (*CollectionResolverMemd, error) {
	if opts == nil {
		opts = &CollectionResolverMemdOptions{}
	}

	return &CollectionResolverMemd{
		logger:         loggerOrNop(opts.Logger),
		clientProvider: opts.ClientProvider,
	}, nil
}

func (cr *CollectionResolverMemd) ResolveCollectionID(
	ctx context.Context, scopeName, collectionName string,
) (collectionId uint32, manifestRev uint64, err error) {
	resp, err := OrchestrateKvClient(
		ctx, cr.clientProvider,
		func(client KvClient) (*memdx.GetCollectionIDResponse, error) {
			return client.GetCollectionID(ctx, &memdx.GetCollectionIDRequest{
				ScopeName:      scopeName,
				CollectionName: collectionName,
			})
		})
	if err != nil {
		return 0, 0, err
	}

	return resp.CollectionID, resp.ManifestRev, nil
}

func (cr *CollectionResolverMemd) InvalidateCollectionID(
	ctx context.Context, scopeName, collectionName, endpoint string, manifestRev uint64,
) {
	// Every collection resolution request yields a new operation to the server
	// meaning every resolution request is 'guarenteed' to be up to date.
}
