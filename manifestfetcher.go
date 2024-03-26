package gocbcorex

import (
	"context"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
)

type ManifestFetcher interface {
	GetManifest(ctx context.Context, bucketName string) (*cbconfig.CollectionManifestJson, error)
}
