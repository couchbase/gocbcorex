package cbmgmtx

import (
	"context"
	"errors"
	"net/http"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type EnsureUserGroupHelper struct {
	Logger     *zap.Logger
	UserAgent  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo

	GroupName   string
	WantMissing bool

	confirmedEndpoints []string
}

func (e *EnsureUserGroupHelper) pollOne(
	ctx context.Context,
	httpRoundTripper http.RoundTripper,
	target NodeTarget,
) (bool, error) {
	e.Logger.Debug("polling a single target",
		zap.String("endpoint", target.Endpoint),
		zap.String("username", target.Username),
		zap.String("groupName", e.GroupName),
		zap.Bool("wantMissing", e.WantMissing))

	_, err := Management{
		Transport: httpRoundTripper,
		UserAgent: e.UserAgent,
		Endpoint:  target.Endpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: target.Username,
			Password: target.Password,
		},
	}.GetUserGroup(ctx, &GetUserGroupOptions{
		GroupName:  e.GroupName,
		OnBehalfOf: e.OnBehalfOf,
	})
	if err != nil {
		if errors.Is(err, ErrGroupNotFound) {
			e.Logger.Debug("target responded with group not found")
			if !e.WantMissing {
				return false, nil
			}
			return true, nil
		}

		e.Logger.Debug("target responded with an unexpected error", zap.Error(err))
		return false, err
	}

	if e.WantMissing {
		e.Logger.Debug("target responded successfully but we wanted a missing group")
		return false, nil
	}

	e.Logger.Debug("target responded successfully")
	return true, nil
}

type EnsureUserGroupPollOptions struct {
	Transport http.RoundTripper
	Targets   []NodeTarget
}

func (e *EnsureUserGroupHelper) Poll(ctx context.Context, opts *EnsureUserGroupPollOptions) (bool, error) {
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
		return false, nil
	}

	return true, nil
}
