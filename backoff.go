package core

import (
	"math"
	"time"
)

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
