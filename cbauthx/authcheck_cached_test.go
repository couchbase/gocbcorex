package cbauthx

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthCheckCache_NoCacheExternal(t *testing.T) {
	authChecks := 0

	auth := NewAuthCheckCached(&AuthCheckMock{
		CheckUserPassFunc: func(ctx context.Context, username, password string) (UserInfo, error) {
			authChecks++

			if username == "local" && password == "valid" {
				return UserInfo{Domain: "local"}, nil
			} else if username == "admin" && password == "valid" {
				return UserInfo{Domain: "admin"}, nil
			} else if username == "ldap" && password == "valid" {
				return UserInfo{Domain: "ldap"}, nil
			}
			return UserInfo{}, ErrInvalidAuth
		},
	})

	// check that 'local' domain is cached
	info, err := auth.CheckUserPass(context.Background(), "local", "valid")
	require.NoError(t, err)
	assert.Equal(t, info.Domain, "local")
	assert.Equal(t, 1, authChecks)

	info, err = auth.CheckUserPass(context.Background(), "local", "valid")
	require.NoError(t, err)
	assert.Equal(t, info.Domain, "local")
	assert.Equal(t, 1, authChecks)

	info, err = auth.CheckUserPass(context.Background(), "local", "invalid")
	require.ErrorIs(t, err, ErrInvalidAuth)
	assert.Equal(t, 1, authChecks)

	// check that 'admin' domain is cached
	info, err = auth.CheckUserPass(context.Background(), "admin", "valid")
	require.NoError(t, err)
	assert.Equal(t, info.Domain, "admin")
	assert.Equal(t, 2, authChecks)

	info, err = auth.CheckUserPass(context.Background(), "admin", "valid")
	require.NoError(t, err)
	assert.Equal(t, info.Domain, "admin")
	assert.Equal(t, 2, authChecks)

	info, err = auth.CheckUserPass(context.Background(), "admin", "invalid")
	require.ErrorIs(t, err, ErrInvalidAuth)
	assert.Equal(t, 2, authChecks)

	// check that 'external' auth is not cached
	info, err = auth.CheckUserPass(context.Background(), "ldap", "valid")
	require.NoError(t, err)
	assert.Equal(t, info.Domain, "ldap")
	assert.Equal(t, 3, authChecks)

	info, err = auth.CheckUserPass(context.Background(), "ldap", "valid")
	require.NoError(t, err)
	assert.Equal(t, info.Domain, "ldap")
	assert.Equal(t, 4, authChecks)

	// check that invalid user auth is not cached
	info, err = auth.CheckUserPass(context.Background(), "invalid", "valid")
	require.ErrorIs(t, err, ErrInvalidAuth)
	assert.Equal(t, 5, authChecks)

	info, err = auth.CheckUserPass(context.Background(), "invalid", "valid")
	require.ErrorIs(t, err, ErrInvalidAuth)
	assert.Equal(t, 6, authChecks)

	// check that invalid password auth is not cached
	info, err = auth.CheckUserPass(context.Background(), "invalid", "invalid")
	require.ErrorIs(t, err, ErrInvalidAuth)
	assert.Equal(t, 7, authChecks)

	info, err = auth.CheckUserPass(context.Background(), "invalid", "invalid")
	require.ErrorIs(t, err, ErrInvalidAuth)
	assert.Equal(t, 8, authChecks)
}
