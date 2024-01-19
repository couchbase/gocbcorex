package cbauthx_test

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbauthx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCbAuthBasicSlow(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfOlderServerVersion(t, "7.2.0")

	logger := zap.Must(zap.NewDevelopment())

	// Connecting to an invalid host should return an error
	_, err := cbauthx.NewCbAuth(context.Background(), &cbauthx.CbAuthConfig{
		Endpoints: []string{
			"http://localhost:9999",
		},
		Username:    testutilsint.TestOpts.Username,
		Password:    testutilsint.TestOpts.Password,
		ClusterUuid: "",
	}, &cbauthx.CbAuthOptions{
		Logger:            logger,
		ServiceName:       "stg",
		UserAgent:         "cng-test",
		HeartbeatInterval: 3 * time.Second,
		HeartbeatTimeout:  5 * time.Second,
		LivenessTimeout:   6 * time.Second,
		ConnectTimeout:    3 * time.Second,
	})
	require.Error(t, err)

	auth, err := cbauthx.NewCbAuth(context.Background(), &cbauthx.CbAuthConfig{
		Endpoints: []string{
			"http://" + testutilsint.TestOpts.HTTPAddrs[0],
		},
		Username:    testutilsint.TestOpts.Username,
		Password:    testutilsint.TestOpts.Password,
		ClusterUuid: "",
	}, &cbauthx.CbAuthOptions{
		Logger:            logger,
		ServiceName:       "stg",
		UserAgent:         "cng-test",
		HeartbeatInterval: 3 * time.Second,
		HeartbeatTimeout:  5 * time.Second,
		LivenessTimeout:   6 * time.Second,
		ConnectTimeout:    3 * time.Second,
	})
	require.NoError(t, err)

	// Check that a valid user works
	userInfo, err := auth.CheckUserPass(context.Background(),
		testutilsint.TestOpts.Username,
		testutilsint.TestOpts.Password)
	require.NoError(t, err)
	assert.Subset(t, []string{"admin", "local"}, []string{userInfo.Domain})

	// Check that an invalid user fails
	_, err = auth.CheckUserPass(context.Background(), "baduser", "badpass")
	require.ErrorIs(t, err, cbauthx.ErrInvalidAuth)

	// Reconfigure to something invalid
	err = auth.Reconfigure(&cbauthx.CbAuthConfig{
		Endpoints: []string{
			"http://localhost:9999",
		},
		Username:    testutilsint.TestOpts.Username,
		Password:    testutilsint.TestOpts.Password,
		ClusterUuid: "",
	})
	require.NoError(t, err)

	// The reconfigure should be internally failing, leading to a
	// continued ability to keep using the existing source.
	_, err = auth.CheckUserPass(context.Background(),
		testutilsint.TestOpts.Username,
		testutilsint.TestOpts.Password)
	require.NoError(t, err)

	_, err = auth.CheckUserPass(context.Background(), "baduser", "badpass")
	require.ErrorIs(t, err, cbauthx.ErrInvalidAuth)

	// Wait 7 seconds for our liveness to be invalidated.
	time.Sleep(7 * time.Second)

	// Now bad things should start happening...
	_, err = auth.CheckUserPass(context.Background(),
		testutilsint.TestOpts.Username,
		testutilsint.TestOpts.Password)
	require.Error(t, err)
	// should fail with reference to cbauth being unavailable
	assert.ErrorContains(t, err, "cbauth unavailability")
	// should include the details on why its unavailable
	assert.ErrorContains(t, err, "failed to dial")

	_, err = auth.CheckUserPass(context.Background(), "baduser", "badpass")
	require.Error(t, err)
	// should fail with reference to cbauth being unavailable
	assert.ErrorContains(t, err, "cbauth unavailability")
	// should include the details on why its unavailable
	assert.ErrorContains(t, err, "failed to dial")

	auth.Close()
}