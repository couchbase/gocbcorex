package core

import (
	"context"

	"github.com/couchbase/stellar-nebula/contrib/cbconfig"
)

type ConfigPoller interface {
	Watch(ctx context.Context, bucketName string) (<-chan *cbconfig.TerseConfigJson, error)
}
