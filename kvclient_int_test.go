package gocbcorex_test

import (
	"context"
	"testing"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestKvClientSelectBucket(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	auth := &gocbcorex.PasswordAuthenticator{
		Username: testutilsint.TestOpts.Username,
		Password: testutilsint.TestOpts.Password,
	}

	cli, err := gocbcorex.NewKvClient(context.Background(), &gocbcorex.KvClientOptions{
		Logger:        logger,
		Address:       testutilsint.TestOpts.MemdAddrs[0],
		Authenticator: auth,
	})
	require.NoError(t, err)

	// Select a bucket on a gcccp level request
	err = cli.SelectBucket(context.Background(), testutilsint.TestOpts.BucketName)
	require.NoError(t, err)

	// Check that an op works
	setRes, err := cli.Set(context.Background(), &memdx.SetRequest{
		Key:       []byte(uuid.NewString()),
		VbucketID: 1,
		Value:     []byte("test"),
	})
	// We don't know if we sent the Set to the correct node for the vbucket so check that the result is either ok or
	// is a NMVB error.
	if err == nil {
		assert.NotZero(t, setRes.Cas)
	} else {
		assert.ErrorIs(t, err, memdx.ErrNotMyVbucket)
	}

	err = cli.Close()
	require.NoError(t, err)
}

func TestKvClientCloseAfterSelectBucket(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	auth := &gocbcorex.PasswordAuthenticator{
		Username: testutilsint.TestOpts.Username,
		Password: testutilsint.TestOpts.Password,
	}

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	cli, err := gocbcorex.NewKvClient(context.Background(), &gocbcorex.KvClientOptions{
		Logger:        logger,
		Address:       testutilsint.TestOpts.MemdAddrs[0],
		TlsConfig:     nil,
		Authenticator: auth,
	})
	require.NoError(t, err)

	err = cli.SelectBucket(context.Background(), testutilsint.TestOpts.BucketName)
	require.NoError(t, err)

	err = cli.Close()
	require.NoError(t, err)
}

func TestKvClientSelectBucketOverExistingBucket(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	auth := &gocbcorex.PasswordAuthenticator{
		Username: testutilsint.TestOpts.Username,
		Password: testutilsint.TestOpts.Password,
	}

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	cli, err := gocbcorex.NewKvClient(context.Background(), &gocbcorex.KvClientOptions{
		Logger:         logger,
		Address:        testutilsint.TestOpts.MemdAddrs[0],
		TlsConfig:      nil,
		Authenticator:  auth,
		SelectedBucket: testutilsint.TestOpts.BucketName,
	})
	require.NoError(t, err)

	err = cli.SelectBucket(context.Background(), "imnotarealboy")
	require.Error(t, err)

	err = cli.Close()
	require.NoError(t, err)
}
