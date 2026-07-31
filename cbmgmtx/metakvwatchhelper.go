package cbmgmtx

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
)

type MetaKvWatchHelper struct {
	Logger       *zap.Logger
	UserAgent    string
	OnBehalfOf   *cbhttpx.OnBehalfOfInfo
	PollInterval time.Duration
	Path         string // default is "/" if empty
}

type MetaKvWatchOptions struct {
	Management Management
}

func (h *MetaKvWatchHelper) Watch(ctx context.Context, opts *MetaKvWatchOptions) (<-chan struct{}, error) {
	pollInterval := h.PollInterval
	if pollInterval <= 0 {
		pollInterval = 2500 * time.Millisecond
	}

	watchPath := h.Path
	if watchPath == "" {
		watchPath = "/"
	}

	ch := make(chan struct{}, 1)

	go func() {
		defer close(ch)

		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		var lastRev string
		hasInitial := false

		checkAndEmit := func() {
			resp, err := opts.Management.GetMetaKv2(ctx, &GetMetaKv2Options{
				Path:       watchPath,
				Recursive:  true,
				OnBehalfOf: h.OnBehalfOf,
			})
			var currentRev string

			if err == nil && resp != nil {
				currentRev = resp.Revision
			} else if errors.Is(err, ErrMetaKvEntryNotFound) {
				currentRev = "not_found"
			} else {
				if h.Logger != nil && err != nil {
					h.Logger.Debug("metakv watch poll failed", zap.Error(err))
				}
				return
			}

			if !hasInitial {
				hasInitial = true
				lastRev = currentRev
				select {
				case ch <- struct{}{}:
				default:
				}
			} else if currentRev != lastRev {
				lastRev = currentRev
				select {
				case ch <- struct{}{}:
				default:
				}
			}
		}

		// Initial poll to emit the first result
		checkAndEmit()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				checkAndEmit()
			}
		}
	}()

	return ch, nil
}
