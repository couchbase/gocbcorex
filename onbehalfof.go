package gocbcorex

import (
	"fmt"
	"strings"
)

// memdExternalUserPrefix is the prefix memcached uses in the on-behalf-of
// frame value to indicate that the user belongs to the external domain
// (LDAP / saslauthd) rather than the local domain.
const memdExternalUserPrefix = "^"

// OnBehalfOfInfo identifies the user that an operation should be executed on
// behalf of.
//
// Domain is the user's authentication domain as reported by cbauth ("local",
// "admin" or "external") and must always be set. memcached resolves a bare
// username against the local user database, so dropping the domain would cause
// an external user to be authorized as a same-named local user.
type OnBehalfOfInfo struct {
	Username string
	Domain   string
}

// EncodeMemdOnBehalfOf encodes an OnBehalfOfInfo into the value memcached
// expects in the on-behalf-of (impersonate user) frame. A nil info encodes to
// an empty string (no impersonation).
//
// Encoding fails closed: an unknown or missing domain, or a username that
// cannot be represented unambiguously, returns an error wrapping
// ErrInvalidArgument rather than falling back to a bare (local) username.
func EncodeMemdOnBehalfOf(info *OnBehalfOfInfo) (string, error) {
	if info == nil {
		return "", nil
	}

	if info.Username == "" {
		return "", fmt.Errorf("%w: on-behalf-of username must not be empty", ErrInvalidArgument)
	}

	switch info.Domain {
	case "local", "admin":
		// memcached would interpret a leading '^' as marking an external
		// user, so a local user with such a name cannot be impersonated
		// without being resolved as a different identity.
		if strings.HasPrefix(info.Username, memdExternalUserPrefix) {
			return "", fmt.Errorf("%w: on-behalf-of username for domain %s must not start with '%s'",
				ErrInvalidArgument, info.Domain, memdExternalUserPrefix)
		}

		return info.Username, nil
	case "external":
		return memdExternalUserPrefix + info.Username, nil
	case "":
		return "", fmt.Errorf("%w: on-behalf-of domain must be specified", ErrInvalidArgument)
	default:
		return "", fmt.Errorf("%w: unsupported on-behalf-of domain: %s", ErrInvalidArgument, info.Domain)
	}
}
