package memdx

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/require"
)

const defaultTestVbucketID = 1

func createTestClient(t *testing.T) *Client {
	testAddress := testutils.TestOpts.MemdAddrs[0]
	testUsername := testutils.TestOpts.Username
	testPassword := testutils.TestOpts.Password
	testBucket := testutils.TestOpts.BucketName

	cli, resp := dialAndBootstrapClient(t, testAddress, testUsername, testPassword, testBucket)

	// As we tie commands to a vbucket we have to ensure that the client we're returning is
	// actually connected to the right node.
	type vbucketServerMap struct {
		ServerList []string `json:"serverList"`
		VBucketMap [][]int  `json:"vBucketMap,omitempty"`
	}
	type cbConfig struct {
		VBucketServerMap vbucketServerMap `json:"vBucketServerMap"`
	}
	var config cbConfig
	require.NoError(t, json.Unmarshal(resp.ClusterConfig.Config, &config))

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

func dialAndBootstrapClient(t *testing.T, addr, user, pass, bucket string) (*Client, *BootstrapResult) {
	conn, err := DialConn(context.Background(), addr, nil)
	require.NoError(t, err, "failed to dial connection")

	cli := NewClient(conn, &ClientOptions{
		OrphanHandler: nil,
		CloseHandler:  nil,
	})

	resp, err := syncUnaryCall(OpBootstrap{
		Encoder: OpsCore{},
	}, OpBootstrap.Bootstrap, cli, &BootstrapOptions{
		Hello: &HelloRequest{
			ClientName: []byte("memdx-test-harness"),
			RequestedFeatures: []HelloFeature{
				HelloFeatureCollections,
				HelloFeatureJSON,
				HelloFeatureSeqNo,
				HelloFeatureXattr,
				HelloFeatureXerror,
				HelloFeatureSyncReplication,
			},
		},
		GetErrorMap: &GetErrorMapRequest{
			Version: 2,
		},
		Auth: &SaslAuthAutoOptions{
			Username:     user,
			Password:     pass,
			EnabledMechs: []AuthMechanism{PlainAuthMechanism, ScramSha512AuthMechanism, ScramSha256AuthMechanism},
		},
		SelectBucket: &SelectBucketRequest{
			BucketName: bucket,
		},
		GetClusterConfig: &GetClusterConfigRequest{},
	})
	require.NoError(t, err, "failed to bootstrap")

	return cli, resp
}
