package memdx_test

import (
	"context"
	"net"
	"testing"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/require"
)

const defaultTestVbucketID = 1

func createTestClient(t *testing.T) *memdx.Client {
	testAddress := testutilsint.TestOpts.MemdAddrs[0]
	testUsername := testutilsint.TestOpts.Username
	testPassword := testutilsint.TestOpts.Password
	testBucket := testutilsint.TestOpts.BucketName

	cli, resp := dialAndBootstrapClient(t, testAddress, testUsername, testPassword, testBucket)

	// As we tie commands to a vbucket we have to ensure that the client we're returning is
	// actually connected to the right node.
	nodeHostname, _, _ := net.SplitHostPort(testAddress)
	config, err := cbconfig.ParseTerseConfig(resp.ClusterConfig.Config, nodeHostname)
	require.NoError(t, err)

	// This is all a bit rough and can be improved, in time.
	vbIdx := config.VBucketServerMap.VBucketMap[defaultTestVbucketID][0]
	address := config.VBucketServerMap.ServerList[vbIdx]

	if testAddress != address {
		err := cli.Close()
		require.NoError(t, err)

		cli, _ = dialAndBootstrapClient(t, address, testUsername, testPassword, testBucket)
	}

	t.Cleanup(func() {
		err := cli.Close()
		require.NoError(t, err)
	})

	return cli
}

func dialAndBootstrapClient(t *testing.T, addr, user, pass, bucket string) (*memdx.Client, *memdx.BootstrapResult) {
	conn, err := memdx.DialConn(context.Background(), addr, nil)
	require.NoError(t, err, "failed to dial connection")

	cli := memdx.NewClient(conn, &memdx.ClientOptions{
		OrphanHandler: nil,
		CloseHandler:  nil,
	})

	resp, err := memdx.SyncUnaryCall(memdx.OpBootstrap{
		Encoder: memdx.OpsCore{},
	}, memdx.OpBootstrap.Bootstrap, cli, &memdx.BootstrapOptions{
		Hello: &memdx.HelloRequest{
			ClientName: []byte("memdx-test-harness"),
			RequestedFeatures: []memdx.HelloFeature{
				memdx.HelloFeatureCollections,
				memdx.HelloFeatureJSON,
				memdx.HelloFeatureSeqNo,
				memdx.HelloFeatureXattr,
				memdx.HelloFeatureXerror,
				memdx.HelloFeatureSyncReplication,
				memdx.HelloFeatureSnappy,
			},
		},
		GetErrorMap: &memdx.GetErrorMapRequest{
			Version: 2,
		},
		Auth: &memdx.SaslAuthAutoOptions{
			Username: user,
			Password: pass,
			EnabledMechs: []memdx.AuthMechanism{
				memdx.PlainAuthMechanism,
				memdx.ScramSha512AuthMechanism,
				memdx.ScramSha256AuthMechanism},
		},
		SelectBucket: &memdx.SelectBucketRequest{
			BucketName: bucket,
		},
		GetClusterConfig: &memdx.GetClusterConfigRequest{},
	})
	require.NoError(t, err, "failed to bootstrap")

	return cli, resp
}
