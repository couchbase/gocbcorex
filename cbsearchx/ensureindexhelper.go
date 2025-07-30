package cbsearchx

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type EnsureIndexHelper struct {
	Logger     *zap.Logger
	UserAgent  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo

	BucketName  string
	ScopeName   string
	IndexName   string
	WantMissing bool
	IndexUUID   string

	confirmedEndpoints []string

	firstRefreshPoll   time.Time
	refreshedEndpoints []string
}

type NodeTarget struct {
	Endpoint string
	Username string
	Password string
}

type EnsureIndexPollOptions struct {
	Transport http.RoundTripper
	Targets   []NodeTarget
}

func (e *EnsureIndexHelper) pollOne(
	ctx context.Context,
	httpRoundTripper http.RoundTripper, target NodeTarget,
) (bool, error) {
	e.Logger.Debug("polling a single target",
		zap.String("endpoint", target.Endpoint),
		zap.String("username", target.Username))

	resp, err := Search{
		Transport: httpRoundTripper,
		UserAgent: e.UserAgent,
		Endpoint:  target.Endpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: target.Username,
			Password: target.Password,
		},
	}.GetIndex(ctx, &GetIndexOptions{
		BucketName: e.BucketName,
		ScopeName:  e.ScopeName,
		IndexName:  e.IndexName,
		OnBehalfOf: e.OnBehalfOf,
	})
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			if e.WantMissing {
				return true, nil
			} else {
				// Due to ING-691, when we receive a not-found error, we need to consider
				// refreshing the configuration before the index will appear.
				e.maybeRefreshOne(ctx, httpRoundTripper, target)

				return false, nil
			}
		}

		e.Logger.Debug("target responded with an unexpected error", zap.Error(err))
		return false, err
	}

	e.Logger.Debug("target responded successfully")

	if e.IndexUUID != "" && resp.UUID != e.IndexUUID {
		return false, nil
	}

	if !e.WantMissing {
		return true, nil
	} else {
		return false, nil
	}
}

func (e *EnsureIndexHelper) maybeRefreshOne(
	ctx context.Context,
	httpRoundTripper http.RoundTripper, target NodeTarget,
) {
	if e.firstRefreshPoll.IsZero() {
		e.firstRefreshPoll = time.Now()
		return
	}

	if e.firstRefreshPoll.Add(5 * time.Second).After(time.Now()) {
		return
	}

	if slices.Contains(e.refreshedEndpoints, target.Endpoint) {
		// this endpoint is already refreshed
		return
	}

	e.Logger.Debug("attempting to refresh configuration for fts node",
		zap.String("endpoint", target.Endpoint))

	err := Search{
		Transport: httpRoundTripper,
		UserAgent: e.UserAgent,
		Endpoint:  target.Endpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: target.Username,
			Password: target.Password,
		},
	}.RefreshConfig(ctx, &RefreshConfigOptions{
		OnBehalfOf: e.OnBehalfOf,
	})
	if err != nil {
		e.Logger.Debug("target responded with an unexpected refresh error", zap.Error(err))
	}

	e.refreshedEndpoints = append(e.refreshedEndpoints, target.Endpoint)
}

func (e *EnsureIndexHelper) Poll(ctx context.Context, opts *EnsureIndexPollOptions) (bool, error) {
	filteredTargets := make([]NodeTarget, 0, len(opts.Targets))
	for _, target := range opts.Targets {
		if !slices.Contains(e.confirmedEndpoints, target.Endpoint) {
			filteredTargets = append(filteredTargets, target)
		}
	}

	var successEndpoints []string
	for _, target := range filteredTargets {
		res, err := e.pollOne(ctx, opts.Transport, target)
		if err != nil {
			return false, err
		}

		if !res {
			continue
		}

		successEndpoints = append(successEndpoints, target.Endpoint)
	}

	e.confirmedEndpoints = append(e.confirmedEndpoints, successEndpoints...)

	if len(successEndpoints) != len(filteredTargets) {
		// some of the endpoints still need to be successful
		return false, nil
	}

	return true, nil
}
