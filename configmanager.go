package core

import (
	"github.com/couchbase/stellar-nebula/contrib/cbconfig"
)

type ConfigManager interface {
	ApplyConfig(sourceHostname string, json *cbconfig.TerseConfigJson)
}
