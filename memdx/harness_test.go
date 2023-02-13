package memdx

import (
	"context"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/require"
	"testing"
)

func createTestClient(t *testing.T) *Client {
	testAddress := testutils.TestOpts.MemdAddrs[0]
	testUsername := testutils.TestOpts.Username
	testPassword := testutils.TestOpts.Password
	testBucket := testutils.TestOpts.BucketName

	conn, err := DialConn(context.Background(), testAddress, nil)
	require.NoError(t, err, "failed to dial connection")

	cli := NewClient(conn, &ClientOptions{
		OrphanHandler: nil,
		CloseHandler:  nil,
	})

	_, err = syncUnaryCall(OpBootstrap{
		Encoder: OpsCore{},
	}, OpBootstrap.Bootstrap, cli, &BootstrapOptions{
		Hello: &HelloRequest{
			ClientName:        []byte("memdx-test-harness"),
			RequestedFeatures: []HelloFeature{HelloFeatureCollections, HelloFeatureJSON, HelloFeatureSeqNo, HelloFeatureXattr},
		},
		GetErrorMap: &GetErrorMapRequest{
			Version: 2,
		},
		Auth: &SaslAuthAutoOptions{
			Username:     testUsername,
			Password:     testPassword,
			EnabledMechs: []AuthMechanism{PlainAuthMechanism, ScramSha512AuthMechanism, ScramSha256AuthMechanism},
		},
		SelectBucket: &SelectBucketRequest{
			BucketName: testBucket,
		},
		GetClusterConfig: &GetClusterConfigRequest{},
	})
	require.NoError(t, err, "failed to bootstrap")

	t.Cleanup(func() {
		cli.Close()
	})

	return cli
}
