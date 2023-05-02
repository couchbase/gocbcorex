package gocbcorex

import (
	"context"
	"crypto/tls"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func CreateDefaultAgentOptions() AgentOptions {
	logger, _ := zap.NewDevelopment()

	return AgentOptions{
		Logger:     logger,
		TLSConfig:  nil,
		BucketName: testutils.TestOpts.BucketName,
		Authenticator: &PasswordAuthenticator{
			Username: testutils.TestOpts.Username,
			Password: testutils.TestOpts.Password,
		},
		SeedConfig: SeedConfig{
			HTTPAddrs: testutils.TestOpts.HTTPAddrs,
			MemdAddrs: testutils.TestOpts.MemdAddrs,
		},
		CompressionConfig: CompressionConfig{
			EnableCompression: true,
		},
	}
}

func CreateDefaultAgent(t *testing.T) *Agent {
	opts := CreateDefaultAgentOptions()

	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)

	return agent
}

func TestAgentBasic(t *testing.T) {
	testutils.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)

	upsertRes, err := agent.Upsert(context.Background(), &UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	getRes, err := agent.Get(context.Background(), &GetOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)
	assert.NotZero(t, getRes.Cas)
	assert.NotEmpty(t, getRes.Value)

	err = agent.Close()
	require.NoError(t, err)
}

func TestAgentBasicTLSInsecure(t *testing.T) {
	testutils.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	opts.TLSConfig = &tls.Config{
		RootCAs:            nil,
		InsecureSkipVerify: true,
	}

	memdHosts, httpHosts := testutils.FudgeConnStringToTLS(t, testutils.TestOpts.OriginalConnStr)

	opts.SeedConfig.MemdAddrs = memdHosts
	opts.SeedConfig.HTTPAddrs = httpHosts

	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)

	upsertRes, err := agent.Upsert(context.Background(), &UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	getRes, err := agent.Get(context.Background(), &GetOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)
	assert.NotZero(t, getRes.Cas)
	assert.NotEmpty(t, getRes.Value)

	err = agent.Close()
	require.NoError(t, err)
}

func TestAgentReconfigureNoTLSToTLS(t *testing.T) {
	t.Skip("Skipping test as Reconfigure not fully implemented yet")
	testutils.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)

	upsertRes, err := agent.Upsert(context.Background(), &UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	err = agent.Reconfigure(&AgentReconfigureOptions{
		TLSConfig: &tls.Config{
			RootCAs:            nil,
			InsecureSkipVerify: true,
		},
		Authenticator: opts.Authenticator,
		BucketName:    opts.BucketName,
	})
	require.NoError(t, err)

	upsertRes, err = agent.Upsert(context.Background(), &UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	err = agent.Close()
	require.NoError(t, err)
}

func TestAgentReconfigureNoBucketToBucket(t *testing.T) {
	t.Skip("Skipping test as Reconfigure not fully implemented yet")
	testutils.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	opts.BucketName = ""
	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)

	_, err = agent.Upsert(context.Background(), &UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.Error(t, err)

	err = agent.Reconfigure(&AgentReconfigureOptions{
		TLSConfig:     opts.TLSConfig,
		Authenticator: opts.Authenticator,
		BucketName:    testutils.TestOpts.BucketName,
	})
	require.NoError(t, err)

	upsertRes, err := agent.Upsert(context.Background(), &UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	err = agent.Close()
	require.NoError(t, err)
}

func TestAgentBadCollection(t *testing.T) {
	testutils.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)

	_, err := agent.Get(context.Background(), &GetOptions{
		Key:            []byte("test"),
		ScopeName:      "invalid-scope",
		CollectionName: "invalid-collection",
	})
	require.ErrorIs(t, err, memdx.ErrUnknownScopeName)

	_, err = agent.Get(context.Background(), &GetOptions{
		Key:            []byte("test"),
		ScopeName:      "_default",
		CollectionName: "invalid-collection",
	})
	require.ErrorIs(t, err, memdx.ErrUnknownCollectionName)

	err = agent.Close()
	require.NoError(t, err)
}

func TestAgentCrudCompress(t *testing.T) {
	testutils.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	opts.CompressionConfig.DisableDecompression = true
	opts.CompressionConfig.MinRatio = 1
	opts.CompressionConfig.MinSize = 1

	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	defer agent.Close()

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
				_, err := agent.Upsert(context.Background(), &UpsertOptions{
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
				_, err := agent.Replace(context.Background(), &ReplaceOptions{
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
				_, err := agent.Add(context.Background(), &AddOptions{
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
				_, err := agent.Append(context.Background(), &AppendOptions{
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
				_, err := agent.Prepend(context.Background(), &PrependOptions{
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
				_, err := agent.Upsert(context.Background(), &UpsertOptions{
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
				get, err := agent.GetMeta(context.Background(), &GetMetaOptions{
					Key:            key,
					ScopeName:      "",
					CollectionName: "",
				})
				require.NoError(tt, err)
				assert.Equal(tt, memdx.DatatypeFlagCompressed, get.Datatype)
			}

			get2, err := agent.Get(context.Background(), &GetOptions{
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
	testutils.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	opts.CompressionConfig.MinRatio = 1
	opts.CompressionConfig.MinSize = 1

	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	defer agent.Close()

	type test struct {
		Op   func(key []byte) ([]byte, error)
		Name string
	}

	tests := []test{
		{
			Name: "Get",
			Op: func(key []byte) ([]byte, error) {
				res, err := agent.Get(context.Background(), &GetOptions{
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
				res, err := agent.GetAndLock(context.Background(), &GetAndLockOptions{
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
				res, err := agent.GetAndTouch(context.Background(), &GetAndTouchOptions{
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

			_, err := agent.Upsert(context.Background(), &UpsertOptions{
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
	testutils.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	defer agent.Close()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()
	configCh := agent.WatchConfig(ctx)

	config, ok := <-configCh
	require.True(t, ok)
	require.NotNil(t, config)
}

func TestAgentConnectAfterCreateBucket(t *testing.T) {
	testutils.SkipIfShortTest(t)

	opts := CreateDefaultAgentOptions()
	opts.BucketName = ""

	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	defer agent.Close()

	bucketName := "testBucket-" + uuid.NewString()[:6]

	err = agent.CreateBucket(context.Background(), &cbmgmtx.CreateBucketOptions{
		BucketName: bucketName,
		BucketSettings: cbmgmtx.BucketSettings{
			MutableBucketSettings: cbmgmtx.MutableBucketSettings{
				RAMQuotaMB: 100,
				BucketType: cbmgmtx.BucketTypeCouchbase,
			},
		},
	})
	require.NoError(t, err)
	defer agent.DeleteBucket(context.Background(), &cbmgmtx.DeleteBucketOptions{
		BucketName: bucketName,
	})

	opts = CreateDefaultAgentOptions()
	opts.BucketName = bucketName
	opts.RetryManager = NewRetryManagerDefault()

	agent2, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	defer agent2.Close()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(30*time.Second))
	defer cancel()

	upsertRes, err := agent2.Upsert(ctx, &UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)
}

func BenchmarkBasicGet(b *testing.B) {
	opts := AgentOptions{
		TLSConfig: nil,
		Authenticator: &PasswordAuthenticator{
			Username: testutils.TestOpts.Username,
			Password: testutils.TestOpts.Password,
		},
		BucketName: testutils.TestOpts.BucketName,
		SeedConfig: SeedConfig{
			HTTPAddrs: testutils.TestOpts.HTTPAddrs,
			MemdAddrs: testutils.TestOpts.MemdAddrs,
		},
	}

	agent, err := CreateAgent(context.Background(), opts)
	if err != nil {
		b.Errorf("failed to create agent: %s", err)
	}
	defer agent.Close()

	_, err = agent.Upsert(context.Background(), &UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	if err != nil {
		b.Errorf("failed to upsert test document: %s", err)
	}

	for n := 0; n < b.N; n++ {
		agent.Get(context.Background(), &GetOptions{
			Key:            []byte("test"),
			ScopeName:      "",
			CollectionName: "",
		})
	}
	b.ReportAllocs()

}
