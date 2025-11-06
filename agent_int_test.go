package gocbcorex_test

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func CreateDefaultAgentOptions() gocbcorex.AgentOptions {
	logger, _ := zap.NewDevelopment()

	return gocbcorex.AgentOptions{
		Logger:     logger,
		TLSConfig:  nil,
		BucketName: testutilsint.TestOpts.BucketName,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: testutilsint.TestOpts.HTTPAddrs,
			MemdAddrs: testutilsint.TestOpts.MemdAddrs,
		},
		CompressionConfig: gocbcorex.CompressionConfig{
			EnableCompression: true,
		},
	}
}

func CreateDefaultAgent(t *testing.T) *gocbcorex.Agent {
	opts := CreateDefaultAgentOptions()

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)

	return agent
}

func TestAgentBasic(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	upsertRes, err := agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	getRes, err := agent.Get(context.Background(), &gocbcorex.GetOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)
	assert.NotZero(t, getRes.Cas)
	assert.NotEmpty(t, getRes.Value)
}

func TestAgentBasicTLSInsecure(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	opts.TLSConfig = &tls.Config{
		RootCAs:            nil,
		InsecureSkipVerify: true,
	}

	memdHosts, httpHosts := testutilsint.FudgeConnStringToTLS(t, testutilsint.TestOpts.OriginalConnStr)

	opts.SeedConfig.MemdAddrs = memdHosts
	opts.SeedConfig.HTTPAddrs = httpHosts

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	upsertRes, err := agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	getRes, err := agent.Get(context.Background(), &gocbcorex.GetOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)
	assert.NotZero(t, getRes.Cas)
	assert.NotEmpty(t, getRes.Value)
}

func TestAgentReconfigureNoTLSToTLS(t *testing.T) {
	t.Skip("Skipping test as Reconfigure not fully implemented yet")
	testutilsint.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	upsertRes, err := agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	err = agent.Reconfigure(&gocbcorex.AgentReconfigureOptions{
		TLSConfig: &tls.Config{
			RootCAs:            nil,
			InsecureSkipVerify: true,
		},
		Authenticator: opts.Authenticator,
		BucketName:    opts.BucketName,
	})
	require.NoError(t, err)

	upsertRes, err = agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)
}

func TestAgentReconfigureNoBucketToBucket(t *testing.T) {
	t.Skip("Skipping test as Reconfigure not fully implemented yet")
	testutilsint.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	opts.BucketName = ""
	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	_, err = agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.Error(t, err)

	err = agent.Reconfigure(&gocbcorex.AgentReconfigureOptions{
		TLSConfig:     opts.TLSConfig,
		Authenticator: opts.Authenticator,
		BucketName:    testutilsint.TestOpts.BucketName,
	})
	require.NoError(t, err)

	upsertRes, err := agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)
}

func TestAgentBadCollection(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	_, err := agent.Get(context.Background(), &gocbcorex.GetOptions{
		Key:            []byte("test"),
		ScopeName:      "invalid-scope",
		CollectionName: "invalid-collection",
	})
	require.ErrorIs(t, err, memdx.ErrUnknownScopeName)

	_, err = agent.Get(context.Background(), &gocbcorex.GetOptions{
		Key:            []byte("test"),
		ScopeName:      "_default",
		CollectionName: "invalid-collection",
	})
	require.ErrorIs(t, err, memdx.ErrUnknownCollectionName)
}

func TestAgentCrudCompress(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	opts.CompressionConfig.DisableDecompression = true
	opts.CompressionConfig.MinRatio = 1
	opts.CompressionConfig.MinSize = 1

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	type test struct {
		Op                    func(key, value []byte) error
		Name                  string
		SkipDocCreation       bool
		Expect                []byte
		ExpectNotDecompressed bool
	}

	value := []byte("aaaa" + strings.Repeat("b", 32) + "aaaabbbb")
	compressed := snappy.Encode(nil, value)
	tests := []test{
		{
			Name: "Upsert",
			Op: func(key, value []byte) error {
				_, err := agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
					Key:            key,
					ScopeName:      "",
					CollectionName: "",
					Value:          value,
				})

				return err
			},
			Expect: compressed,
		},
		{
			Name: "Replace",
			Op: func(key, value []byte) error {
				_, err := agent.Replace(context.Background(), &gocbcorex.ReplaceOptions{
					Key:            key,
					ScopeName:      "",
					CollectionName: "",
					Value:          value,
				})

				return err
			},
			Expect: compressed,
		},
		{
			Name: "Add",
			Op: func(key, value []byte) error {
				_, err := agent.Add(context.Background(), &gocbcorex.AddOptions{
					Key:            key,
					ScopeName:      "",
					CollectionName: "",
					Value:          value,
				})

				return err
			},
			SkipDocCreation: true,
			Expect:          compressed,
		},
		{
			Name: "Append",
			Op: func(key, value []byte) error {
				_, err := agent.Append(context.Background(), &gocbcorex.AppendOptions{
					Key:            key,
					ScopeName:      "",
					CollectionName: "",
					Value:          value,
				})

				return err
			},
			Expect:                append(value, value...),
			ExpectNotDecompressed: true,
		},
		{
			Name: "Prepend",
			Op: func(key, value []byte) error {
				_, err := agent.Prepend(context.Background(), &gocbcorex.PrependOptions{
					Key:            key,
					ScopeName:      "",
					CollectionName: "",
					Value:          value,
				})

				return err
			},
			Expect:                append(value, value...),
			ExpectNotDecompressed: true,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			key := []byte(uuid.NewString()[:6])

			if !test.SkipDocCreation {
				_, err := agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
					Key:            key,
					ScopeName:      "",
					CollectionName: "",
					Value:          value,
				})
				require.NoError(tt, err)
			}

			err := test.Op(key, value)
			require.NoError(tt, err)

			if !test.ExpectNotDecompressed {
				get, err := agent.GetMeta(context.Background(), &gocbcorex.GetMetaOptions{
					Key:            key,
					ScopeName:      "",
					CollectionName: "",
					FetchDatatype:  true,
				})
				require.NoError(tt, err)

				require.NotNil(tt, get.Datatype)
				assert.Equal(tt, memdx.DatatypeFlagCompressed, *get.Datatype)
			}

			get2, err := agent.Get(context.Background(), &gocbcorex.GetOptions{
				Key:            key,
				ScopeName:      "",
				CollectionName: "",
			})
			require.NoError(tt, err)

			assert.Equal(tt, test.Expect, get2.Value)
		})
	}
}

func TestAgentCrudDecompress(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	opts.CompressionConfig.MinRatio = 1
	opts.CompressionConfig.MinSize = 1

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	type test struct {
		Op   func(key []byte) ([]byte, error)
		Name string
	}

	tests := []test{
		{
			Name: "Get",
			Op: func(key []byte) ([]byte, error) {
				res, err := agent.Get(context.Background(), &gocbcorex.GetOptions{
					Key:            key,
					ScopeName:      "",
					CollectionName: "",
				})
				if err != nil {
					return nil, err
				}

				return res.Value, nil
			},
		},
		{
			Name: "GetAndLock",
			Op: func(key []byte) ([]byte, error) {
				res, err := agent.GetAndLock(context.Background(), &gocbcorex.GetAndLockOptions{
					Key:            key,
					ScopeName:      "",
					CollectionName: "",
				})
				if err != nil {
					return nil, err
				}

				return res.Value, nil
			},
		},
		{
			Name: "GetAndTouch",
			Op: func(key []byte) ([]byte, error) {
				res, err := agent.GetAndTouch(context.Background(), &gocbcorex.GetAndTouchOptions{
					Key:            key,
					ScopeName:      "",
					CollectionName: "",
					Expiry:         1,
				})
				if err != nil {
					return nil, err
				}

				return res.Value, nil
			},
		},
	}

	value := []byte("aaaa" + strings.Repeat("b", 32) + "aaaabbbb")
	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			key := []byte(uuid.NewString()[:6])

			_, err := agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
				Key:            key,
				ScopeName:      "",
				CollectionName: "",
				Value:          value,
			})
			require.NoError(tt, err)

			resVal, err := test.Op(key)
			require.NoError(tt, err)

			assert.Equal(tt, value, resVal)

		})
	}
}

func TestAgentWatchConfig(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()
	configCh := agent.WatchConfig(ctx)

	config, ok := <-configCh
	require.True(t, ok)
	require.NotNil(t, config)
}

func TestAgentBucketNotExist(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	opts.BucketName = uuid.NewString()[:6]

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	assert.ErrorIs(t, err, cbmgmtx.ErrBucketNotFound)

	if agent != nil {
		err = agent.Close()
		require.NoError(t, err)
	}
}

func TestAgentConnectAfterCreateBucket(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	opts.BucketName = ""

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	bucketName := "testBucket-" + uuid.NewString()[:6]

	err = agent.CreateBucket(context.Background(), &cbmgmtx.CreateBucketOptions{
		BucketName: bucketName,
		BucketSettings: cbmgmtx.BucketSettings{
			MutableBucketSettings: cbmgmtx.MutableBucketSettings{
				RAMQuotaMB: 100,
			},
			BucketType: cbmgmtx.BucketTypeCouchbase,
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		err := agent.DeleteBucket(context.Background(), &cbmgmtx.DeleteBucketOptions{
			BucketName: bucketName,
		})
		require.NoError(t, err)
	})

	// we have to run this in an eventually loop as gocbcorex itself does not
	// provide any direct guarantees about the consistency of bucket states.
	// if the bucket is not found during the runs, we simply retry a little
	// bit later.
	require.Eventually(t, func() bool {
		opts = CreateDefaultAgentOptions()
		opts.BucketName = bucketName
		opts.RetryManager = gocbcorex.NewRetryManagerDefault()

		agent2, err := gocbcorex.CreateAgent(context.Background(), opts)
		if errors.Is(err, memdx.ErrUnknownBucketName) {
			return false
		}
		require.NoError(t, err)
		t.Cleanup(func() {
			err := agent2.Close()
			require.NoError(t, err)
		})

		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(30*time.Second))
		defer cancel()

		upsertRes, err := agent2.Upsert(ctx, &gocbcorex.UpsertOptions{
			Key:            []byte("test"),
			ScopeName:      "",
			CollectionName: "",
			Value:          []byte(`{"foo": "bar"}`),
		})
		if errors.Is(err, memdx.ErrUnknownBucketName) {
			return false
		}

		require.NoError(t, err)
		assert.NotZero(t, upsertRes.Cas)

		return true
	}, 30*time.Second, 100*time.Millisecond)
}

func TestAgentSetWithMetaVBUUID(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)

	key := uuid.NewString()[:6]

	upsertRes, err := agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)

	_, err = agent.SetWithMeta(context.Background(), &gocbcorex.SetWithMetaOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
		VBUUID:         12345,
	})
	require.ErrorIs(t, err, gocbcorex.ErrVbucketUUIDMismatch)

	var uuidMismatchErr *gocbcorex.VbucketUUIDMismatchError
	require.ErrorAs(t, err, &uuidMismatchErr)

	actualUUID := uuidMismatchErr.ActualVbUUID

	_, err = agent.SetWithMeta(context.Background(), &gocbcorex.SetWithMetaOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
		VBUUID:         actualUUID,
		StoreCas:       upsertRes.Cas,
		Options:        memdx.MetaOpFlagSkipConflictResolution,
	})
	require.NoError(t, err)

	err = agent.Close()
	require.NoError(t, err)
}

func BenchmarkBasicGet(b *testing.B) {
	opts := gocbcorex.AgentOptions{
		TLSConfig: nil,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
		BucketName: testutilsint.TestOpts.BucketName,
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: testutilsint.TestOpts.HTTPAddrs,
			MemdAddrs: testutilsint.TestOpts.MemdAddrs,
		},
	}

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	if err != nil {
		b.Errorf("failed to create agent: %s", err)
	}
	b.Cleanup(func() {
		err := agent.Close()
		if err != nil {
			b.Errorf("failed to close agent: %s", err)
		}
	})

	_, err = agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	if err != nil {
		b.Errorf("failed to upsert test document: %s", err)
	}

	for n := 0; n < b.N; n++ {
		_, err := agent.Get(context.Background(), &gocbcorex.GetOptions{
			Key:            []byte("test"),
			ScopeName:      "",
			CollectionName: "",
		})
		if err != nil {
			b.Errorf("failed to get test document: %s", err)
		}
	}
	b.ReportAllocs()
}

func TestAgentStaticInfo(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()
	mode, err := agent.GetConflictResolutionMode(ctx)
	require.NoError(t, err)

	assert.NotEqual(t, cbmgmtx.ConflictResolutionTypeUnset, mode)
}
