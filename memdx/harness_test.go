package memdx

import (
	"context"
	"github.com/couchbase/stellar-nebula/core/testutils"
	"testing"
)

func createTestClient(t *testing.T) *Client {
	testAddress := testutils.TestOpts.MemdAddrs[0]
	testUsername := "Administrator"
	testPassword := "password"
	testBucket := "default"

	conn, err := DialConn(context.Background(), testAddress, nil)
	if err != nil {
		t.Fatalf("failed to dial connection: %s", err)
	}

	cli := NewClient(conn, &ClientOptions{
		OrphanHandler: nil,
		CloseHandler:  nil,
	})

	syncUnaryCall(OpBootstrap{
		Encoder: OpsCore{},
	}, OpBootstrap.Bootstrap, cli, &BootstrapOptions{
		Hello: &HelloRequest{
			ClientName:        []byte("memdx-test-harness"),
			RequestedFeatures: []HelloFeature{HelloFeatureCollections},
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
	if err != nil {
		cli.Close()
		t.Fatalf("failed to bootstrap: %s", err)
	}

	t.Cleanup(func() {
		cli.Close()
	})

	return cli
}
