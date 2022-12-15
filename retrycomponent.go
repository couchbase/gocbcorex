package core

import (
	"math"
	"time"
)

type RetryComponent interface {
	OrchestrateRetries(ctx *asyncContext, dispatchCb func(func(error), error)) error
}

type retryComponent struct {
	calc BackoffCalculator
}

func newRetryComponent() *retryComponent {
	return &retryComponent{
		calc: ExponentialBackoff(0, 500*time.Millisecond, 1),
	}
}

func (rc *retryComponent) OrchestrateRetries(ctx *asyncContext, dispatchCb func(func(error), error)) error {
	var handler func(error)
	var retries uint32
	handler = func(err error) {
		cancelled := make(chan struct{})
		ctx.OnCancel(func(err error) {
			close(cancelled)
			dispatchCb(nil, err)
		})
		retries++
		timer := time.AfterFunc(rc.calc(retries), func() {
			ctx.DropOnCancel()
			dispatchCb(handler, nil)
		})

		select {
		case <-cancelled:
			ctx.DropOnCancel()
			timer.Stop()
		case <-timer.C:
		}
	}
	dispatchCb(handler, nil)
	return nil
}

// BackoffCalculator is used by retry strategies to calculate backoff durations.
type BackoffCalculator func(retryAttempts uint32) time.Duration

func ExponentialBackoff(min, max time.Duration, backoffFactor float64) BackoffCalculator {
	var minBackoff float64 = 1000000   // 1 Millisecond
	var maxBackoff float64 = 500000000 // 500 Milliseconds
	var factor float64 = 2

	if min > 0 {
		minBackoff = float64(min)
	}
	if max > 0 {
		maxBackoff = float64(max)
	}
	if backoffFactor > 0 {
		factor = backoffFactor
	}

	return func(retryAttempts uint32) time.Duration {
		backoff := minBackoff * (math.Pow(factor, float64(retryAttempts)))

		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		if backoff < minBackoff {
			backoff = minBackoff
		}

		return time.Duration(backoff)
	}
}
