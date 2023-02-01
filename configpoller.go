package core

import (
	"context"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
)

type TerseConfigJsonWithSource struct {
	Config         *cbconfig.TerseConfigJson
	SourceHostname string
}

type ConfigPoller interface {
	Watch(ctx context.Context) (<-chan *TerseConfigJsonWithSource, error)
}
