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

func TestDcpBasic(t *testing.T) {
	ctx := context.Background()
	testutilsint.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	auth := &gocbcorex.PasswordAuthenticator{
		Username: testutilsint.TestOpts.Username,
		Password: testutilsint.TestOpts.Password,
	}

	cli, err := gocbcorex.NewDcpStreamClient(ctx, &gocbcorex.DcpStreamClientOptions{
		Address:         testutilsint.TestOpts.MemdAddrs[0],
		ClientName:      "test-client",
		Authenticator:   auth,
		Logger:          logger,
		Bucket:          "default",
		ConnectionName:  "test-conn",
		ConnectionFlags: memdx.DcpConnectionFlagsProducer,
		NoopInterval:    5 * time.Second,
		Handlers: gocbcorex.DcpEventsHandlers{
			StreamOpen: func(evt *memdx.DcpStreamReqResponse) {
				log.Printf("StreamOpen: %+v", evt)
			},
			StreamEnd: func(evt *memdx.DcpStreamEndEvent) {
				log.Printf("StreamEnd: %+v", evt)
			},
			SnapshotMarker: func(evt *memdx.DcpSnapshotMarkerEvent) {
				log.Printf("SnapshotMarker: %+v", evt)
			},
			ScopeCreation: func(evt *memdx.DcpScopeCreationEvent) {
				log.Printf("ScopeCreation: %+v", evt)
			},
			CollectionCreation: func(evt *memdx.DcpCollectionCreationEvent) {
				log.Printf("CollectionCreation: %+v", evt)
			},
			Mutation: func(evt *memdx.DcpMutationEvent) {
				log.Printf("Mutation: %s (value-len: %d)", evt.Key, len(evt.Value))
			},
		},
	})
	require.NoError(t, err)

	err = cli.OpenStream(ctx, &memdx.DcpStreamReqRequest{
		VbucketID:      0,
		Flags:          0,
		StartSeqNo:     0,
		EndSeqNo:       0xffffffffffffffff,
		VbUuid:         0,
		SnapStartSeqNo: 0,
		SnapEndSeqNo:   0xffffffffffffffff,
	})
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	err = cli.CloseStream(ctx, &memdx.DcpCloseStreamRequest{
		VbucketID: 0,
	})
	require.NoError(t, err)

	err = cli.Close()
	require.NoError(t, err)
}
