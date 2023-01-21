package core

import (
	"context"
	"time"
)

func contextSleep(ctx context.Context, period time.Duration) error {
	select {
	case <-time.After(period):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
