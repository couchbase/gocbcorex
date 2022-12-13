package core

import "github.com/couchbase/gocbcore/v10/memd"

type RetryComponent struct {
}

func (rc *RetryComponent) Dispatch(ctx *asyncContext, dispatchCb func(*asyncContext, finalCallback), finalCb finalCallback) {
	dispatchCb(ctx, rc.makeFinalCallback(dispatchCb))
}

func (rc *RetryComponent) makeFinalCallback(dispatchCb func(*asyncContext, finalCallback)) func(ctx *asyncContext, pak *memd.Packet, client interface{}, err error) bool {
	return func(ctx *asyncContext, pak *memd.Packet, client interface{}, err error) bool {
		return rc.handleRetry(dispatchCb)
	}
}

func (rc *RetryComponent) handleRetry(dispatchCb func(*asyncContext, finalCallback)) bool {

}
