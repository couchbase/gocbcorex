package gocbcorex_test

import (
	"context"
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

	cli, err := gocbcorex.NewDcpClient(ctx, &gocbcorex.DcpClientOptions{
		Logger:          logger,
		Address:         testutilsint.TestOpts.MemdAddrs[0],
		Authenticator:   auth,
		SelectedBucket:  "default",
		ConnectionName:  "test-conn",
		ConnectionFlags: memdx.DcpConnectionFlagsProducer,
		NoopInterval:    5 * time.Second,
		Handlers: gocbcorex.DcpClientEventsHandlers{
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
		},
	})
	require.NoError(t, err)

	streamResp, err := cli.DcpStreamReq(ctx, &memdx.DcpStreamReqRequest{
		VbucketID:      0,
		Flags:          0,
		StartSeqNo:     0,
		EndSeqNo:       0xffffffffffffffff,
		VbUuid:         0,
		SnapStartSeqNo: 0,
		SnapEndSeqNo:   0xffffffffffffffff,
	}, func(resp *memdx.DcpStreamReqResponse) error {
		log.Printf("StreamReq sync response: %+v", resp)
		return nil
	})
	require.NoError(t, err)
	log.Printf("StreamReq response: %+v", streamResp)

	streamResp, err = cli.DcpStreamReq(ctx, &memdx.DcpStreamReqRequest{
		VbucketID:      0,
		Flags:          0,
		StartSeqNo:     0,
		EndSeqNo:       0xffffffffffffffff,
		VbUuid:         0,
		SnapStartSeqNo: 0,
		SnapEndSeqNo:   0xffffffffffffffff,
	}, func(resp *memdx.DcpStreamReqResponse) error {
		log.Printf("StreamReq sync response: %+v", resp)
		return nil
	})
	require.NoError(t, err)
	log.Printf("StreamReq response: %+v", streamResp)

	time.Sleep(10 * time.Second)

	closeResp, err := cli.DcpCloseStream(ctx, &memdx.DcpCloseStreamRequest{
		VbucketID: 0,
	})
	require.NoError(t, err)
	log.Printf("CloseStream response: %+v", closeResp)

	err = cli.Close()
	require.NoError(t, err)
}
