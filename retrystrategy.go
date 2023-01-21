package core

import "time"

type RetryStrategy interface {
	GetRetryAction(err error) time.Duration
}
