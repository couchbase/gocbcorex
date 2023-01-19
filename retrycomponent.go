package core

import (
	"time"
)

type RetryComponent interface {
	OrchestrateRetries(ctx *AsyncContext, dispatchCb func(func(error), error)) error
}

type retryComponent struct {
	calc BackoffCalculator
}

func newRetryComponent() *retryComponent {
	return &retryComponent{
		calc: ExponentialBackoff(0, 500*time.Millisecond, 1),
	}
}

func (rc *retryComponent) OrchestrateRetries(ctx *AsyncContext, dispatchCb func(func(error), error)) error {
	var handler func(error)
	var retries uint32
	handler = func(err error) {
		cancelCtx := ctx.WithCancellation()
		timer := time.AfterFunc(rc.calc(retries), func() {
			if !cancelCtx.MarkComplete() {
				return
			}

			// dispatch no-error
		})
		cancelCtx.OnCancel(func(err error) bool {
			if !timer.Stop() {
				return false
			}

			// dispatch err
			return true
		})

		dispatchCb(nil, err)
	}
	dispatchCb(handler, nil)
	return nil
}
