package core

import (
	"context"

	"github.com/couchbase/stellar-nebula/contrib/cbconfig"
)

type TerseConfigJsonWithSource struct {
	Config         *cbconfig.TerseConfigJson
	SourceHostname string
}

type ConfigPoller interface {
	Watch(ctx context.Context) (<-chan *TerseConfigJsonWithSource, error)
	UpdateEndpoints(endpoints []string)
}
