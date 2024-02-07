package gocbcorex

import (
	"context"

	"github.com/couchbase/gocbcorex/cbmgmtx"
)

type BucketCheckerMgmt struct {
	Mgmt *MgmtComponent
}

var _ BucketChecker = (*BucketCheckerMgmt)(nil)

func (c *BucketCheckerMgmt) HasBucket(ctx context.Context, bucketName string) (bool, error) {
	return c.Mgmt.CheckBucketExists(ctx, &cbmgmtx.CheckBucketExistsOptions{
		BucketName: bucketName,
	})
}
