package gocbcorex_test

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDcpClientBasic(t *testing.T) {
	ctx := context.Background()
	testutilsint.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	auth := &gocbcorex.PasswordAuthenticator{
		Username: testutilsint.TestOpts.Username,
		Password: testutilsint.TestOpts.Password,
	}

	cli, err := gocbcorex.NewKvClient(ctx, &gocbcorex.KvClientConfig{
		Address:                testutilsint.TestOpts.MemdAddrs[0],
		Authenticator:          auth,
		SelectedBucket:         "default",
		DisableDefaultFeatures: true,
		ExtraFeatures: []memdx.HelloFeature{
			memdx.HelloFeatureXerror,
			memdx.HelloFeatureDatatype,
			memdx.HelloFeatureSeqNo,
			memdx.HelloFeatureXattr,
			memdx.HelloFeatureSnappy,
			memdx.HelloFeatureJSON,
			memdx.HelloFeatureDurations,
			memdx.HelloFeaturePreserveExpiry,
			memdx.HelloFeatureSyncReplication,
			memdx.HelloFeatureReplaceBodyWithXattr,
			memdx.HelloFeatureSelectBucket,
			memdx.HelloFeatureCreateAsDeleted,
			memdx.HelloFeatureAltRequests,
			memdx.HelloFeatureCollections,
		},
	}, &gocbcorex.KvClientOptions{
		Logger: logger,
		UnsolicitedHandlers: &memdx.UnsolicitedOpsHandlers{
			DcpSnapshotMarker: func(evt *memdx.DcpSnapshotMarkerEvent) error {
				log.Printf("SnapshotMarker: %+v", evt)
				return nil
			},
			DcpScopeCreation: func(evt *memdx.DcpScopeCreationEvent) error {
				log.Printf("ScopeCreation: %+v", evt)
				return nil
			},
			DcpCollectionCreation: func(evt *memdx.DcpCollectionCreationEvent) error {
				log.Printf("CollectionCreation: %+v", evt)
				return nil
			},
			DcpMutation: func(evt *memdx.DcpMutationEvent) error {
				log.Printf("Mutation: %s (value-len: %d)", evt.Key, len(evt.Value))
				return nil
			},
			DcpStreamEnd: func(evt *memdx.DcpStreamEndEvent) error {
				log.Printf("StreamEnd: %+v", evt)
				return nil
			},
			DcpNoOp: func(evt *memdx.DcpNoOpEvent) (*memdx.DcpNoOpEventResponse, error) {
				log.Printf("NoOp: %+v", evt)
				return &memdx.DcpNoOpEventResponse{}, nil
			},
		},
	})
	require.NoError(t, err)

	_, err = cli.DcpOpenConnection(ctx, &memdx.DcpOpenConnectionRequest{
		ConnectionName: "test-connection",
		ConsumerName:   "",
		Flags:          memdx.DcpConnectionFlagsProducer,
	})
	require.NoError(t, err)

	_, err = cli.DcpControl(ctx, &memdx.DcpControlRequest{
		Key:   "enable_noop",
		Value: "true",
	})
	require.NoError(t, err)

	noopPeriod := 5 * time.Second
	_, err = cli.DcpControl(ctx, &memdx.DcpControlRequest{
		Key:   "set_noop_interval",
		Value: fmt.Sprintf("%d", noopPeriod/time.Second),
	})
	require.NoError(t, err)

	resp, err := cli.DcpStreamReq(ctx, &memdx.DcpStreamReqRequest{
		VbucketID:      0,
		Flags:          0,
		StartSeqNo:     0,
		EndSeqNo:       0xffffffffffffffff,
		VbUuid:         0,
		SnapStartSeqNo: 0,
		SnapEndSeqNo:   0xffffffffffffffff,
	})
	require.NoError(t, err)
	log.Printf("StreamReq response: %+v", resp)

	time.Sleep(10 * time.Second)

	err = cli.Close()
	require.NoError(t, err)
}
