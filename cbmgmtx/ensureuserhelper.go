package cbmgmtx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

// WantUserSettings specifies expected role and/or group assignments to wait
// for, as part of an EnsureUser call. Either field can be left nil to skip
// checking that particular dimension.
type WantUserSettings struct {
	// Roles, if set, causes Poll to wait until each target's directly
	// assigned roles (i.e. excluding any roles inherited via group
	// membership) match this set exactly.
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
		if e.WantSettings.Roles != nil {
			matched, err := rolesMatch(resp.Roles, e.WantSettings.Roles)
			if err != nil {
				return false, err
			}
			if !matched {
				e.Logger.Debug("target responded with success, but the roles have not yet propagated")
				return false, nil
			}
		}
		if e.WantSettings.Groups != nil && !setsEqual(resp.Groups, e.WantSettings.Groups) {
			e.Logger.Debug("target responded with success, but the groups have not yet propagated")
			return false, nil
		}
	}

	e.Logger.Debug("target responded successfully")
	return true, nil
}

// parseRoleSpec parses a role spec string in the same encoded form used by
// UpsertUserOptions.Roles (e.g. "data_reader[beer-sample:my_scope:my_collection]")
// back into its component parts, so it can be compared structurally against
// a role decoded from a GetUser response.
func parseRoleSpec(spec string) (RoleJson, error) {
	bracketIdx := strings.Index(spec, "[")
	if bracketIdx == -1 {
		return RoleJson{RoleName: spec}, nil
	}

	if !strings.HasSuffix(spec, "]") {
		return RoleJson{}, fmt.Errorf("invalid role spec %q: missing closing ']'", spec)
	}

	roleName := spec[:bracketIdx]
	scoping := spec[bracketIdx+1 : len(spec)-1]

	parts := strings.Split(scoping, ":")
	if len(parts) > 3 {
		return RoleJson{}, fmt.Errorf("invalid role spec %q: too many ':'-separated parts", spec)
	}
	if parts[0] == "" {
		return RoleJson{}, fmt.Errorf("invalid role spec %q: empty bucket name", spec)
	}

	role := RoleJson{
		RoleName:   roleName,
		BucketName: parts[0],
	}
	if len(parts) > 1 {
		role.ScopeName = parts[1]
	}
	if len(parts) > 2 {
		role.CollectionName = parts[2]
	}

	return role, nil
}

// rolesMatch reports whether actual's directly-assigned roles (i.e.
// excluding any roles inherited via group membership) are exactly the set of
// roles named in want.
func rolesMatch(actual []RoleWithOriginsJson, want []string) (bool, error) {
	var actualUserRoles []RoleJson
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

		actualUserRoles = append(actualUserRoles, role.RoleJson)
	}

	wantRoles := make([]RoleJson, len(want))
	for i, spec := range want {
		role, err := parseRoleSpec(spec)
		if err != nil {
			return false, err
		}
		wantRoles[i] = role
	}

	return setsEqual(actualUserRoles, wantRoles), nil
}

// setsEqual reports whether a and b contain the same elements, regardless of
// order.
func setsEqual[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}

	counts := make(map[T]int, len(a))
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		counts[v]--
	}
	for _, c := range counts {
		if c != 0 {
			return false
		}
	}

	return true
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
