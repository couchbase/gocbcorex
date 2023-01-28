package core

import (
	"github.com/couchbase/stellar-nebula/contrib/cbconfig"
)

type RouteConfigHandler func(*routeConfig)

type ConfigManager interface {
	RegisterCallback(fn RouteConfigHandler)
	ApplyConfig(sourceHostname string, json *cbconfig.TerseConfigJson)
}
