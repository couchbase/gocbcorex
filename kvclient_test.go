package core

import (
	"context"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestKvClientReconfigureBucket(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	logger, _ := zap.NewDevelopment()

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Logger:   logger,
		Address:  testutils.TestOpts.MemdAddrs[0],
		Username: testutils.TestOpts.Username,
		Password: testutils.TestOpts.Password,
	})
	require.NoError(t, err)

	// Select a bucket on a gcccp level request
	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address:        testutils.TestOpts.MemdAddrs[0],
		Username:       testutils.TestOpts.Username,
		Password:       testutils.TestOpts.Password,
		SelectedBucket: testutils.TestOpts.BucketName,
	})
	require.NoError(t, err)

	// Check that an op works
	setRes, err := cli.Set(context.Background(), &memdx.SetRequest{
		Key:       []byte(uuid.NewString()),
		VbucketID: 1,
		Value:     []byte("test"),
	})
	require.NoError(t, err)
	assert.NotZero(t, setRes.Cas)
}

func TestKvClientReconfigureInvalidBucket(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	logger, _ := zap.NewDevelopment()

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Logger:         logger,
		Address:        testutils.TestOpts.MemdAddrs[0],
		Username:       testutils.TestOpts.Username,
		Password:       testutils.TestOpts.Password,
		SelectedBucket: testutils.TestOpts.BucketName,
	})
	require.NoError(t, err)

	// Select a bucket on a gcccp level request
	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address:        testutils.TestOpts.MemdAddrs[0],
		Username:       testutils.TestOpts.Username,
		Password:       testutils.TestOpts.Password,
		SelectedBucket: "imnotarealboy",
	})
	require.ErrorIs(t, err, memdx.ErrUnknownBucketName)

	// Check that an op still works
	setRes, err := cli.Set(context.Background(), &memdx.SetRequest{
		Key:       []byte(uuid.NewString()),
		VbucketID: 1,
		Value:     []byte("test"),
	})
	require.NoError(t, err)
	assert.NotZero(t, setRes.Cas)
}
