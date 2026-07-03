package cbmgmtx

import (
	"context"
	"errors"
	"net/http"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type EnsureUserHelper struct {
	Logger     *zap.Logger
	UserAgent  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo

	Username    string
	Domain      AuthDomain
	WantMissing bool

	// SincePasswordChanged, if set, causes Poll to additionally wait until
	// each target's password_change_date is after this marker. Leave this nil
	// for any use of EnsureUser that isn't confirming a password change.
	SincePasswordChanged *PasswordChangedMarker

	confirmedEndpoints []string
}

func (e *EnsureUserHelper) pollOne(
	ctx context.Context,
	httpRoundTripper http.RoundTripper,
	target NodeTarget,
) (bool, error) {
	e.Logger.Debug("polling a single target",
		zap.String("endpoint", target.Endpoint),
		zap.String("username", target.Username),
		zap.String("targetUsername", e.Username),
		zap.Bool("wantMissing", e.WantMissing))

	resp, err := Management{
		Transport: httpRoundTripper,
		UserAgent: e.UserAgent,
		Endpoint:  target.Endpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: target.Username,
			Password: target.Password,
		},
	}.GetUser(ctx, &GetUserOptions{
		Username:   e.Username,
		Domain:     e.Domain,
		OnBehalfOf: e.OnBehalfOf,
	})
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			e.Logger.Debug("target responded with user not found")
			if !e.WantMissing {
				return false, nil
			}
			return true, nil
		}

		e.Logger.Debug("target responded with an unexpected error", zap.Error(err))
		return false, err
	}

	if e.WantMissing {
		e.Logger.Debug("target responded successfully but we wanted a missing user")
		return false, nil
	}

	if e.SincePasswordChanged != nil && !resp.PasswordChanged.IsAfter(*e.SincePasswordChanged) {
		e.Logger.Debug("target responded with success, but the password change has not yet propagated")
		return false, nil
	}

	e.Logger.Debug("target responded successfully")
	return true, nil
}

type EnsureUserPollOptions struct {
	Transport http.RoundTripper
	Targets   []NodeTarget
}

func (e *EnsureUserHelper) Poll(ctx context.Context, opts *EnsureUserPollOptions) (bool, error) {
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
