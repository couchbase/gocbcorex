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
)

func TestDcpBasic(t *testing.T) {
	ctx := context.Background()
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	mutsCh := make(chan *memdx.DcpMutationEvent, 1024)
	streamSet, err := agent.NewStreamSet(gocbcorex.NewStreamSetOptions{
		DcpOpts: gocbcorex.KvClientDcpOptions{
			ConnectionName:  "dcp-test-conn",
			ConnectionFlags: memdx.DcpConnectionFlagsProducer,
			NoopInterval:    5 * time.Second,
		},
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
	numVbs := uint16(agent.NumVbuckets())
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
	mut := <-mutsCh
	require.NotNil(t, mut)
	t.Logf("Received mutation: %v", mut)

	for vbId := uint16(0); vbId < numVbs; vbId++ {
		err = streamSet.CloseVbucket(ctx, vbId)
		require.NoError(t, err)
	}
}
