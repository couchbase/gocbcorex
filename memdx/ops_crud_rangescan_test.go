package memdx

import (
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpsCrudRangeScanCreateContinueOnce(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	key := []byte(uuid.NewString())
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)
	defer cli.Close()

	resp, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.Set, cli, &SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    1,
		Value:        value,
		Datatype:     datatype,
	})
	require.NoError(t, err)

	createResp, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.RangeScanCreate, cli, &RangeScanCreateRequest{
		CollectionID: 0,
		VbucketID:    1,
		Range: &RangeScanCreateRangeScanConfig{
			Start: key,
			End:   append(key, "\xFF"...),
		},
		// Server is sometimes returning empty range if we don't set snapshot_requirements.
		Snapshot: &RangeScanCreateSnapshotRequirements{
			VbUUID:   resp.MutationToken.VbUuid,
			SeqNo:    resp.MutationToken.SeqNo,
			Deadline: time.Now().Add(2500 * time.Millisecond),
		},
	})
	require.NoError(t, err)

	scanUUID := createResp.ScanUUUID
	assert.NotEmpty(t, scanUUID)

	continueResp, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.RangeScanContinue, cli, &RangeScanContinueRequest{
		VbucketID: 1,
		ScanUUID:  scanUUID,
	})
	require.NoError(t, err)

	assert.False(t, continueResp.KeysOnly)
	assert.False(t, continueResp.More)
	assert.True(t, continueResp.Complete)

	assert.Len(t, continueResp.Items, 1)
	item := continueResp.Items[0]

	assert.Equal(t, key, item.Key)
	assert.Equal(t, value, item.Value)
	assert.Equal(t, datatype, item.Datatype)
	assert.NotZero(t, item.Cas)
	assert.NotZero(t, item.SeqNo)
	assert.Zero(t, item.Expiry)
	assert.Zero(t, item.Flags)
}

// Failing because of what looks like a server issue.
func TestOpsCrudRangeScanCreateContinueMoreThanOnce(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	baseKey := "s" + uuid.NewString()[:6]
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)
	defer cli.Close()

	var highSeqNo uint64
	var vbUUID uint64
	for i := 0; i < 10; i++ {
		resp, err := syncUnaryCall(OpsCrud{
			CollectionsEnabled: true,
			ExtFramesEnabled:   true,
		}, OpsCrud.Set, cli, &SetRequest{
			CollectionID: 0,
			Key:          []byte(string(baseKey) + "-" + strconv.Itoa(i)),
			VbucketID:    1,
			Value:        value,
			Datatype:     datatype,
		})
		require.NoError(t, err)

		if resp.MutationToken.SeqNo > highSeqNo {
			highSeqNo = resp.MutationToken.SeqNo
			vbUUID = resp.MutationToken.VbUuid
		}
	}

	createResp, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.RangeScanCreate, cli, &RangeScanCreateRequest{
		CollectionID: 0,
		VbucketID:    1,
		Range: &RangeScanCreateRangeScanConfig{
			Start: []byte(baseKey),
			End:   append([]byte(baseKey), "\xFF"...),
		},
		// Server is sometimes returning empty range if we don't set snapshot_requirements.
		Snapshot: &RangeScanCreateSnapshotRequirements{
			VbUUID:   vbUUID,
			SeqNo:    highSeqNo,
			Deadline: time.Now().Add(2500 * time.Millisecond),
		},
	})
	require.NoError(t, err)

	scanUUID := createResp.ScanUUUID
	assert.NotEmpty(t, scanUUID)

	continueResp, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.RangeScanContinue, cli, &RangeScanContinueRequest{
		VbucketID: 1,
		ScanUUID:  scanUUID,
		MaxCount:  1,
	})
	require.NoError(t, err)

	assert.False(t, continueResp.KeysOnly)
	assert.True(t, continueResp.More)
	assert.False(t, continueResp.Complete)

	assert.Len(t, continueResp.Items, 1)
	item := continueResp.Items[0]

	assert.Contains(t, string(item.Key), baseKey)
	assert.Equal(t, value, item.Value)
	assert.Equal(t, datatype, item.Datatype)
	assert.NotZero(t, item.Cas)
	assert.NotZero(t, item.SeqNo)
	assert.Zero(t, item.Expiry)
	assert.Zero(t, item.Flags)
}

func TestOpsCrudRangeScanCreateContinueCancel(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	baseKey := "s" + uuid.NewString()[:6]
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)
	defer cli.Close()

	var highSeqNo uint64
	var vbUUID uint64
	for i := 0; i < 10; i++ {
		resp, err := syncUnaryCall(OpsCrud{
			CollectionsEnabled: true,
			ExtFramesEnabled:   true,
		}, OpsCrud.Set, cli, &SetRequest{
			CollectionID: 0,
			Key:          []byte(string(baseKey) + "-" + strconv.Itoa(i)),
			VbucketID:    1,
			Value:        value,
			Datatype:     datatype,
		})
		require.NoError(t, err)

		if resp.MutationToken.SeqNo > highSeqNo {
			highSeqNo = resp.MutationToken.SeqNo
			vbUUID = resp.MutationToken.VbUuid
		}
	}

	createResp, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.RangeScanCreate, cli, &RangeScanCreateRequest{
		CollectionID: 0,
		VbucketID:    1,
		Range: &RangeScanCreateRangeScanConfig{
			Start: []byte(baseKey),
			End:   append([]byte(baseKey), "-\xFF"...),
		},
		// Server is sometimes returning empty range if we don't set snapshot_requirements.
		Snapshot: &RangeScanCreateSnapshotRequirements{
			VbUUID:   vbUUID,
			SeqNo:    highSeqNo,
			Deadline: time.Now().Add(2500 * time.Millisecond),
		},
	})
	require.NoError(t, err)

	scanUUID := createResp.ScanUUUID
	assert.NotEmpty(t, scanUUID)

	continueResp, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.RangeScanContinue, cli, &RangeScanContinueRequest{
		VbucketID: 1,
		ScanUUID:  scanUUID,
		MaxCount:  1,
	})
	require.NoError(t, err)

	assert.True(t, continueResp.More)
	assert.False(t, continueResp.Complete)

	_, err = syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.RangeScanCancel, cli, &RangeScanCancelRequest{
		ScanUUID:  scanUUID,
		VbucketID: 1,
	})
	require.NoError(t, err)
}
