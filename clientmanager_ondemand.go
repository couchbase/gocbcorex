package gocbcorex

import "go.uber.org/zap"

type OnDemandClientManager[Tclient any] struct {
	logger      *zap.Logger
	newClientFn func() (Tclient, error)
}
