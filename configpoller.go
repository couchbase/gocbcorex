package gocbcorex

import (
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
)

type TerseConfigJsonWithSource struct {
	Config         *cbconfig.TerseConfigJson
	SourceHostname string
}

type ConfigPoller interface {
	Watch() (<-chan *TerseConfigJsonWithSource, error)
	Close() error
}
