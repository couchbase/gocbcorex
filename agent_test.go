package core

import (
	"context"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"strings"
	"testing"
)

func CreateDefaultAgent(t *testing.T) *Agent {
	logger, _ := zap.NewDevelopment()

	opts := AgentOptions{
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
	}

	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)

	return agent
}

func TestAgentBasic(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

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

func TestAgentBadCollection(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

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
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	agent := CreateDefaultAgent(t)

	// TODO: Short term until we expose compression options
	agent.crud.compression.(*CompressionManagerDefault).disableDecompression = true
	agent.crud.compression.(*CompressionManagerDefault).compressionMinRatio = 1
	agent.crud.compression.(*CompressionManagerDefault).compressionMinSize = 1

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
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	agent := CreateDefaultAgent(t)

	// TODO: Short term until we expose compression options
	agent.crud.compression.(*CompressionManagerDefault).disableDecompression = false
	agent.crud.compression.(*CompressionManagerDefault).compressionMinRatio = 1
	agent.crud.compression.(*CompressionManagerDefault).compressionMinSize = 1

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

func TestAgentBasicHTTP(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	agent := CreateDefaultAgent(t)

	resp, err := agent.SendHTTPRequest(context.Background(), &HTTPRequest{
		Service: MgmtService,
		Path:    "/pools/default/nodeServices",
	})
	require.NoError(t, err)

	assert.Equal(t, 200, resp.Raw.StatusCode)

	err = resp.Raw.Body.Close()
	require.NoError(t, err)

	err = agent.Close()
	require.NoError(t, err)
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
