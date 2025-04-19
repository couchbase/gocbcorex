package gocbcorex_test

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
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

	agent := CreateDefaultAgent(t)

	dcpAgent, err := gocbcorex.CreateDcpAgent(ctx, gocbcorex.DcpAgentOptions{
		Logger:        logger,
		TLSConfig:     nil,
		Authenticator: auth,
		BucketName:    testutilsint.TestOpts.BucketName,
		SeedMgmtAddrs: testutilsint.TestOpts.HTTPAddrs,
		StreamOptions: gocbcorex.DcpStreamOptions{
			ConnectionName: "test-conn",
			NoopInterval:   5 * time.Second,
		},
	})
	require.NoError(t, err)

	mutsCh := make(chan *memdx.DcpMutationEvent, 1024)
	streamSet, err := dcpAgent.NewStreamSet(&gocbcorex.NewStreamSetOptions{
		Handlers: gocbcorex.DcpEventsHandlers{
			Mutation: func(evt *memdx.DcpMutationEvent) {
				select {
				case mutsCh <- evt:
				default:
					// Drop the event if the channel is full
				}
			},
		},
	})
	require.NoError(t, err)

	// write something so we know we have at least one mutation
	testKey := []byte("dcp-test-key" + uuid.NewString()[:6])
	_, err = agent.Upsert(ctx, &gocbcorex.UpsertOptions{
		Key:   testKey,
		Value: []byte(testKey),
	})
	require.NoError(t, err)

	// open all vbuckets
	numVbs := dcpAgent.NumVbuckets()
	for vbId := uint16(0); vbId < numVbs; vbId++ {
		_, err := streamSet.OpenVbucket(ctx, &gocbcorex.OpenVbucketOptions{
			VbucketId:      vbId,
			Flags:          0,
			StartSeqNo:     0,
			EndSeqNo:       0xffffffffffffffff,
			VbUuid:         0,
			SnapStartSeqNo: 0,
			SnapEndSeqNo:   0xffffffffffffffff,
		})
		require.NoError(t, err)
	}

	// we should receive at least one mutation
	<-mutsCh

	for vbId := uint16(0); vbId < numVbs; vbId++ {
		err = streamSet.CloseVbucket(ctx, vbId)
		require.NoError(t, err)
	}

	err = dcpAgent.Close()
	require.NoError(t, err)

	err = agent.Close()
	require.NoError(t, err)
}
