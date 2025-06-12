package gocbcorex

import (
	"context"
	"strconv"
	"strings"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"go.uber.org/zap"
)

type CollectionResolverHttpOptions struct {
	Logger        *zap.Logger
	BucketName    string
	MgmtComponent *MgmtComponent
}

type CollectionResolverHttp struct {
	logger     *zap.Logger
	bucketName string
	mgmt       *MgmtComponent
}

var _ CollectionResolver = (*CollectionResolverHttp)(nil)

func NewCollectionResolverHttp(opts *CollectionResolverHttpOptions) (*CollectionResolverHttp, error) {
	if opts == nil {
		opts = &CollectionResolverHttpOptions{}
	}

	return &CollectionResolverHttp{
		logger:     loggerOrNop(opts.Logger),
		bucketName: opts.BucketName,
		mgmt:       opts.MgmtComponent,
	}, nil
}

func (cr *CollectionResolverHttp) ResolveCollectionID(
	ctx context.Context, scopeName, collectionName string,
) (collectionId uint32, manifestRev uint64, err error) {
	manifest, err := cr.mgmt.GetCollectionManifest(ctx, &cbmgmtx.GetCollectionManifestOptions{
		BucketName: cr.bucketName,
	})
	if err != nil {
		return 0, 0, err
	}

	var manifestScope *cbconfig.CollectionManifestScopeJson
	for _, scope := range manifest.Scopes {
		if scope.Name == scopeName {
			manifestScope = &scope
			break
		}
	}
	if manifestScope == nil {
		return 0, 0, cbmgmtx.ErrScopeNotFound
	}

	var manifestColl *cbconfig.CollectionManifestCollectionJson
	for _, coll := range manifestScope.Collections {
		if coll.Name == collectionName {
			manifestColl = &coll
			break
		}
	}
	if manifestColl == nil {
		return 0, 0, cbmgmtx.ErrCollectionNotFound
	}

	parsedManifestRev, err := strconv.ParseUint(strings.ReplaceAll(manifest.UID, "0x", ""), 16, 64)
	if err != nil {
		return 0, 0, err
	}

	parsedCollectionId, err := strconv.ParseUint(strings.ReplaceAll(manifestColl.UID, "0x", ""), 16, 32)
	if err != nil {
		return 0, 0, err
	}

	return uint32(parsedCollectionId), parsedManifestRev, nil
}

func (cr *CollectionResolverHttp) InvalidateCollectionID(
	ctx context.Context, scopeName, collectionName, endpoint string, manifestRev uint64,
) {
	// Every collection resolution request yields a new operation to the server
	// meaning every resolution request is 'guarenteed' to be up to date.
}
