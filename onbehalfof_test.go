package gocbcorex

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeMemdOnBehalfOf(t *testing.T) {
	testCases := []struct {
		name     string
		info     *OnBehalfOfInfo
		expected string
	}{
		{name: "nil", info: nil, expected: ""},
		{name: "local", info: &OnBehalfOfInfo{Username: "alice", Domain: "local"}, expected: "alice"},
		{name: "admin", info: &OnBehalfOfInfo{Username: "Administrator", Domain: "admin"}, expected: "Administrator"},
		{name: "external", info: &OnBehalfOfInfo{Username: "alice", Domain: "external"}, expected: "^alice"},
		{name: "external with caret", info: &OnBehalfOfInfo{Username: "^alice", Domain: "external"}, expected: "^^alice"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeMemdOnBehalfOf(tc.info)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, encoded)
		})
	}
}

func TestEncodeMemdOnBehalfOfInvalid(t *testing.T) {
	testCases := []struct {
		name string
		info *OnBehalfOfInfo
	}{
		{name: "empty username", info: &OnBehalfOfInfo{Username: "", Domain: "local"}},
		// A missing domain must not silently fall back to a local user.
		{name: "missing domain", info: &OnBehalfOfInfo{Username: "alice"}},
		{name: "unknown domain", info: &OnBehalfOfInfo{Username: "alice", Domain: "bucket"}},
		{name: "local_token domain", info: &OnBehalfOfInfo{Username: "alice", Domain: "local_token"}},
		// A local user named '^alice' would be resolved by memcached as the
		// external user 'alice'.
		{name: "local with caret", info: &OnBehalfOfInfo{Username: "^alice", Domain: "local"}},
		{name: "admin with caret", info: &OnBehalfOfInfo{Username: "^alice", Domain: "admin"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeMemdOnBehalfOf(tc.info)
			assert.ErrorIs(t, err, ErrInvalidArgument)
			assert.Empty(t, encoded)
		})
	}
}
