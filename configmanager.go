package core

import (
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
)

type RouteConfigHandler interface {
	HandleRouteConfig(config *routeConfig)
}

type ConfigManager interface {
	RegisterCallback(handler RouteConfigHandler)
	UnregisterCallback(handler RouteConfigHandler)
	ApplyConfig(sourceHostname string, json *cbconfig.TerseConfigJson)
}
