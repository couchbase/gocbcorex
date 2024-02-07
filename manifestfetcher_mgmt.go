package gocbcorex

import (
	"context"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
)

type ManifestFetcherMgmt struct {
	Mgmt *MgmtComponent
}

var _ ManifestFetcher = (*ManifestFetcherMgmt)(nil)

func (c *ManifestFetcherMgmt) GetManifest(ctx context.Context, bucketName string) (*cbconfig.CollectionManifestJson, error) {
	return c.Mgmt.GetCollectionManifest(ctx, &cbmgmtx.GetCollectionManifestOptions{
		BucketName: bucketName,
	})
}
