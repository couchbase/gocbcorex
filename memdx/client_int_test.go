package memdx_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
)

func TestClientOpCancellation(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	t.Skip("Skipping test due to flakeyness")

	key := []byte(uuid.NewString())

	cli := createTestClient(t)

	result := make(chan memdx.UnaryResult[*memdx.Packet], 1)
	expectedErr := errors.New("some error")
	op, err := cli.Dispatch(&memdx.Packet{
		Magic:     memdx.MagicReq,
		OpCode:    memdx.OpCodeGet,
		Key:       key,
		VbucketID: defaultTestVbucketID,
	}, func(packet *memdx.Packet, err error) bool {
		result <- memdx.UnaryResult[*memdx.Packet]{
			Resp: packet,
			Err:  err,
		}

		return false
	})
	require.NoError(t, err)
	op.Cancel(expectedErr)

	res := <-result
	assert.ErrorIs(t, res.Err, expectedErr)
	assert.Nil(t, res.Resp)
}

// This test just tests that cancelling an already handled op doesn't do anything weird.
func TestClientOpCancellationAfterResult(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())

	cli := createTestClient(t)

	result := make(chan memdx.UnaryResult[*memdx.Packet], 1)
	expectedErr := errors.New("some error")
	op, err := cli.Dispatch(&memdx.Packet{
		Magic:     memdx.MagicReq,
		OpCode:    memdx.OpCodeGet,
		Key:       key,
		VbucketID: defaultTestVbucketID,
	}, func(packet *memdx.Packet, err error) bool {
		result <- memdx.UnaryResult[*memdx.Packet]{
			Resp: packet,
			Err:  err,
		}

		return false
	})
	require.NoError(t, err)

	res := <-result
	assert.NoError(t, res.Err)

	op.Cancel(expectedErr)
}
