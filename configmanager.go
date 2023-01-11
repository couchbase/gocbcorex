package core

import "github.com/couchbase/stellar-nebula/contrib/cbconfig"

type ConfigManager interface {
	ApplyConfig(json *cbconfig.TerseConfigJson) (*routeConfig, bool)
}
