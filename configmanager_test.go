package core

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/couchbase/stellar-nebula/contrib/cbconfig"
	"github.com/couchbase/stellar-nebula/core/memdx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrchestrateMemdRoutingReturnsResult(t *testing.T) {
	expectedEndpoint := "endpoint1"
	expectedVbID := uint16(22)
	expectedKey := []byte("aweasel")
	expectedResult := 322

	mock := &VbucketRouterMock{
		DispatchByKeyFunc: func(key []byte) (string, uint16, error) {
			assert.Equal(t, expectedKey, key)
			return expectedEndpoint, expectedVbID, nil
		},
	}

	res, err := OrchestrateMemdRouting(context.Background(), mock, nil, expectedKey, func(endpoint string, vbID uint16) (int, error) {
		assert.Equal(t, expectedEndpoint, endpoint)
		assert.Equal(t, expectedVbID, vbID)

		return expectedResult, nil
	})
	require.NoError(t, err)

	assert.Equal(t, expectedResult, res)
}

func TestOrchestrateConfigReturnsError(t *testing.T) {
	expectedEndpoint := "endpoint1"
	expectedVbID := uint16(22)
	expectedKey := []byte("aweasel")
	expectedErr := errors.New("imanerror")

	mock := &VbucketRouterMock{
		DispatchByKeyFunc: func(key []byte) (string, uint16, error) {
			assert.Equal(t, expectedKey, key)
			return expectedEndpoint, expectedVbID, nil
		},
	}

	_, err := OrchestrateMemdRouting(context.Background(), mock, nil, expectedKey, func(endpoint string, vbID uint16) (int, error) {
		assert.Equal(t, expectedEndpoint, endpoint)
		assert.Equal(t, expectedVbID, vbID)

		return 0, expectedErr
	})
	assert.ErrorIs(t, err, expectedErr)
}

func TestOrchestrateConfigReturnsErrorFromDispatch(t *testing.T) {
	expectedEndpoint := "endpoint1"
	expectedVbID := uint16(22)
	expectedKey := []byte("aweasel")
	expectedErr := errors.New("imanerror")

	mock := &VbucketRouterMock{
		DispatchByKeyFunc: func(key []byte) (string, uint16, error) {
			assert.Equal(t, expectedKey, key)
			return "", 0, expectedErr
		},
	}

	_, err := OrchestrateMemdRouting(context.Background(), mock, nil, expectedKey, func(endpoint string, vbID uint16) (int, error) {
		assert.Equal(t, expectedEndpoint, endpoint)
		assert.Equal(t, expectedVbID, vbID)

		return 0, errors.New("shouldnt have reached here")
	})
	assert.ErrorIs(t, err, expectedErr)
}

func TestOrchestrateConfigNMVBRetriesAndAppliesConfig(t *testing.T) {
	expectedEndpoint := "endpoint1"
	expectedVbID := uint16(22)
	newExpectedVbID := uint16(113)
	expectedKey := []byte("aweasel")
	expectedResult := 322

	cfg := GenTerseClusterConfig(1, 1, []string{expectedEndpoint})
	cfgBytes, err := json.Marshal(cfg)
	require.NoError(t, err)

	var timesDispatched int
	var timesConfigApplied int

	mock := &VbucketRouterMock{
		DispatchByKeyFunc: func(key []byte) (string, uint16, error) {
			timesDispatched++
			assert.Equal(t, expectedKey, key)

			if timesDispatched == 1 {
				return expectedEndpoint, expectedVbID, nil
			} else {
				return expectedEndpoint, newExpectedVbID, nil
			}
		},
	}
	mgrMock := &ConfigManagerMock{
		ApplyConfigFunc: func(sourceHostname string, json *cbconfig.TerseConfigJson) {
			timesConfigApplied++
			assert.Equal(t, expectedEndpoint, sourceHostname)
			assert.Equal(t, cfg, json)
		},
	}

	var timesFnCalled int
	res, err := OrchestrateMemdRouting(context.Background(), mock, mgrMock, expectedKey, func(endpoint string, vbID uint16) (int, error) {
		timesFnCalled++
		assert.Equal(t, expectedEndpoint, endpoint)

		if timesFnCalled == 1 {
			assert.Equal(t, expectedVbID, vbID)
			return 0, memdx.ServerErrorWithConfig{Cause: memdx.ErrNotMyVbucket, ConfigJson: cfgBytes}
		} else if timesFnCalled == 2 {
			assert.Equal(t, newExpectedVbID, vbID)
			return expectedResult, nil
		}

		return 0, errors.New("shouldnt have reached here")
	})
	require.NoError(t, err)

	assert.Equal(t, expectedResult, res)
	assert.Equal(t, 2, timesDispatched)
	assert.Equal(t, 1, timesConfigApplied)
	assert.Equal(t, 2, timesFnCalled)
}
