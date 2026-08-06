package cbmgmtx

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

// WantUserSettings specifies expected role and/or group assignments to wait
// for, as part of an EnsureUser call. Either field can be left nil to skip
// checking that particular dimension - this mirrors EnsureBucketHelper's
// WantSettings, where each field is checked independently.
type WantUserSettings struct {
	// Roles, if set, causes Poll to wait until each target's directly
	// assigned roles (i.e. excluding any roles inherited via group
	// membership) match this set exactly. Use the same encoded strings as
	// UpsertUserOptions.Roles (e.g. "bucket_admin[travel-sample]").
	Roles []string

	// Groups, if set, causes Poll to wait until each target's group
	// memberships match this set exactly.
	Groups []string
}

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

	// WantSettings, if set, causes Poll to additionally wait until each
	// target's roles and/or groups match the given values.
	WantSettings *WantUserSettings

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

	if e.SincePasswordChanged != nil && !resp.PasswordChanged.After(e.SincePasswordChanged.t) {
		e.Logger.Debug("target responded with success, but the password change has not yet propagated")
		return false, nil
	}

	if e.WantSettings != nil {
		if e.WantSettings.Roles != nil && !rolesMatch(resp.Roles, e.WantSettings.Roles) {
			e.Logger.Debug("target responded with success, but the roles have not yet propagated")
			return false, nil
		}
		if e.WantSettings.Groups != nil && !stringSetsEqual(resp.Groups, e.WantSettings.Groups) {
			e.Logger.Debug("target responded with success, but the groups have not yet propagated")
			return false, nil
		}
	}

	e.Logger.Debug("target responded successfully")
	return true, nil
}

// encodeRole renders a RoleJson back into the same encoded string form used
// by UpsertUserOptions.Roles (e.g. "data_reader[beer-sample:my_scope:my_collection]"),
// so that a role decoded from a GetUser response can be compared against the
// roles a caller originally requested.
func encodeRole(role RoleJson) string {
	if role.BucketName == "" {
		return role.RoleName
	}

	scoping := role.BucketName
	if role.ScopeName != "" {
		scoping += ":" + role.ScopeName
		if role.CollectionName != "" {
			scoping += ":" + role.CollectionName
		}
	}

	return fmt.Sprintf("%s[%s]", role.RoleName, scoping)
}

// rolesMatch reports whether actual's directly-assigned roles (i.e.
// excluding any roles inherited via group membership) are exactly the set of
// roles named in want.
func rolesMatch(actual []RoleWithOriginsJson, want []string) bool {
	var actualUserRoles []string
	for _, role := range actual {
		isDirectlyAssigned := false
		for _, origin := range role.Origins {
			if origin.Type == "user" {
				isDirectlyAssigned = true
				break
			}
		}
		if !isDirectlyAssigned {
			continue
		}

		actualUserRoles = append(actualUserRoles, encodeRole(role.RoleJson))
	}

	return stringSetsEqual(actualUserRoles, want)
}

// stringSetsEqual reports whether a and b contain the same strings,
// regardless of order.
func stringSetsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	aCopy := append([]string(nil), a...)
	bCopy := append([]string(nil), b...)

	slices.Sort(aCopy)
	slices.Sort(bCopy)

	return slices.Equal(aCopy, bCopy)
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
