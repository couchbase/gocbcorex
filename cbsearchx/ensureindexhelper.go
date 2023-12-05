package cbsearchx

import (
	"context"
	"errors"
	"net/http"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type EnsureIndexHelper struct {
	Logger     *zap.Logger
	UserAgent  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo

	BucketName string
	ScopeName  string
	IndexName  string

	confirmedEndpoints []string
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

func (e *EnsureIndexHelper) PollCreated(ctx context.Context, opts *EnsureIndexPollOptions) (bool, error) {
	return e.pollAll(ctx, opts, func(exists bool) bool {
		if !exists {
			e.Logger.Debug("target still didn't have the index")
			return false
		}

		return true
	})
}

func (e *EnsureIndexHelper) PollDropped(ctx context.Context, opts *EnsureIndexPollOptions) (bool, error) {
	return e.pollAll(ctx, opts, func(exists bool) bool {
		// If there are rows then the endpoint knows the index.
		if exists {
			e.Logger.Debug(" target still had the index")
			return false
		}

		return true
	})
}

func (e *EnsureIndexHelper) pollOne(
	ctx context.Context,
	httpRoundTripper http.RoundTripper, target NodeTarget,
) (bool, error) {
	e.Logger.Debug("polling a single target",
		zap.String("endpoint", target.Endpoint),
		zap.String("username", target.Username))

	_, err := Search{
		Transport: httpRoundTripper,
		UserAgent: e.UserAgent,
		Endpoint:  target.Endpoint,
		Username:  target.Username,
		Password:  target.Password,
	}.GetIndex(ctx, &GetIndexOptions{
		BucketName: e.BucketName,
		ScopeName:  e.ScopeName,
		IndexName:  e.IndexName,
		OnBehalfOf: e.OnBehalfOf,
	})
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			return false, nil
		}

		e.Logger.Debug("target responded with an unexpected error", zap.Error(err))
		return false, err
	}

	return true, nil
}

func (e *EnsureIndexHelper) pollAll(ctx context.Context,
	opts *EnsureIndexPollOptions, cb func(bool) bool) (bool, error) {
	filteredTargets := make([]NodeTarget, 0, len(opts.Targets))
	for _, target := range opts.Targets {
		if !slices.Contains(e.confirmedEndpoints, target.Endpoint) {
			filteredTargets = append(filteredTargets, target)
		}
	}

	var successEndpoints []string
	for _, target := range filteredTargets {
		resp, err := e.pollOne(ctx, opts.Transport, target)
		if err != nil {
			return false, err
		}

		if resp {
			e.Logger.Debug("target successfully checked")

			successEndpoints = append(successEndpoints, target.Endpoint)
		}
	}

	e.confirmedEndpoints = append(e.confirmedEndpoints, successEndpoints...)

	if len(successEndpoints) != len(filteredTargets) {
		// some of the endpoints still need to be successful
		return false, nil
	}

	return true, nil
}
