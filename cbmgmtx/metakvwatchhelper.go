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

	// We perform the initial poll inline so that we can directly return errors such as
	// the metakv2 endpoints being unsupported (server versions before 8.0.0), rather
	// than silently never emitting any results.
	lastRev, err := h.pollRevision(ctx, opts.Management, watchPath)
	if err != nil {
		return nil, err
	}

	ch := make(chan struct{}, 1)

	// Emit the first result
	ch <- struct{}{}

	go func() {
		defer close(ch)

		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			currentRev, err := h.pollRevision(ctx, opts.Management, watchPath)
			if err != nil {
				if h.Logger != nil {
					h.Logger.Debug("metakv watch poll failed", zap.Error(err))
				}
				continue
			}

			if currentRev != lastRev {
				lastRev = currentRev
				select {
				case ch <- struct{}{}:
				default:
				}
			}
		}
	}()

	return ch, nil
}

func (h *MetaKvWatchHelper) pollRevision(ctx context.Context, mgmt Management, watchPath string) (string, error) {
	resp, err := mgmt.GetMetaKv2(ctx, &GetMetaKv2Options{
		Path:       watchPath,
		Recursive:  true,
		OnBehalfOf: h.OnBehalfOf,
	})
	if errors.Is(err, ErrMetaKvEntryNotFound) {
		// the path not existing yet is not an error, we simply watch for it to appear
		return "not_found", nil
	} else if err != nil {
		return "", err
	}

	return resp.Revision, nil
}
