package memdx_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpsCrudRangeScanCreateContinueOnce(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureRangeScan)

	key := []byte(uuid.NewString())
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)
	t.Cleanup(func() {
		err := cli.Close()
		require.NoError(t, err)
	})

	resp, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Value:        value,
		Datatype:     datatype,
	})
	require.NoError(t, err)

	createResp, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.RangeScanCreate, cli, &memdx.RangeScanCreateRequest{
		CollectionID: 0,
		VbucketID:    defaultTestVbucketID,
		Range: &memdx.RangeScanCreateRangeScanConfig{
			Start: key,
			End:   append(key, "\xFF"...),
		},
		// Server is sometimes returning empty range if we don't set snapshot_requirements.
		Snapshot: &memdx.RangeScanCreateSnapshotRequirements{
			VbUUID:   resp.MutationToken.VbUuid,
			SeqNo:    resp.MutationToken.SeqNo,
			Deadline: time.Now().Add(2500 * time.Millisecond),
		},
	})
	require.NoError(t, err)

	scanUUID := createResp.ScanUUUID
	assert.NotEmpty(t, scanUUID)

	var items []memdx.RangeScanItem
	waitAction := make(chan memdx.UnaryResult[*memdx.RangeScanActionResponse], 1)
	_, err = memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}.RangeScanContinue(cli, &memdx.RangeScanContinueRequest{
		VbucketID: defaultTestVbucketID,
		ScanUUID:  scanUUID,
	}, func(response *memdx.RangeScanDataResponse) error {
		assert.False(t, response.KeysOnly)
		items = append(items, response.Items...)
		return nil
	}, func(response *memdx.RangeScanActionResponse, err error) {
		waitAction <- memdx.UnaryResult[*memdx.RangeScanActionResponse]{
			Resp: response,
			Err:  err,
		}
	})
	require.NoError(t, err)

	action := <-waitAction
	require.NoError(t, action.Err)

	assert.False(t, action.Resp.More)
	assert.True(t, action.Resp.Complete)

	assert.Len(t, items, 1)
	item := items[0]

	assert.Equal(t, key, item.Key)
	assert.Equal(t, value, item.Value)
	assert.Equal(t, datatype, item.Datatype)
	assert.NotZero(t, item.Cas)
	assert.NotZero(t, item.SeqNo)
	assert.Zero(t, item.Expiry)
	assert.Zero(t, item.Flags)
}

func TestOpsCrudRangeScanCreateContinueMoreThanOnce(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureRangeScan)

	baseKey := "s" + uuid.NewString()[:6]
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)
	t.Cleanup(func() {
		err := cli.Close()
		require.NoError(t, err)
	})

	var highSeqNo uint64
	var vbUUID uint64
	for i := 0; i < 10; i++ {
		resp, err := memdx.SyncUnaryCall(memdx.OpsCrud{
			CollectionsEnabled: true,
			ExtFramesEnabled:   true,
		}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
			CollectionID: 0,
			Key:          []byte(string(baseKey) + "-" + strconv.Itoa(i)),
			VbucketID:    defaultTestVbucketID,
			Value:        value,
			Datatype:     datatype,
		})
		require.NoError(t, err)

		if resp.MutationToken.SeqNo > highSeqNo {
			highSeqNo = resp.MutationToken.SeqNo
			vbUUID = resp.MutationToken.VbUuid
		}
	}

	createResp, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.RangeScanCreate, cli, &memdx.RangeScanCreateRequest{
		CollectionID: 0,
		VbucketID:    defaultTestVbucketID,
		Range: &memdx.RangeScanCreateRangeScanConfig{
			Start: []byte(baseKey),
			End:   append([]byte(baseKey), "\xFF"...),
		},
		// Server is sometimes returning empty range if we don't set snapshot_requirements.
		Snapshot: &memdx.RangeScanCreateSnapshotRequirements{
			VbUUID:   vbUUID,
			SeqNo:    highSeqNo,
			Deadline: time.Now().Add(2500 * time.Millisecond),
		},
	})
	require.NoError(t, err)

	scanUUID := createResp.ScanUUUID
	assert.NotEmpty(t, scanUUID)

	var items []memdx.RangeScanItem
	waitAction := make(chan memdx.UnaryResult[*memdx.RangeScanActionResponse], 1)
	_, err = memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}.RangeScanContinue(cli, &memdx.RangeScanContinueRequest{
		VbucketID: defaultTestVbucketID,
		ScanUUID:  scanUUID,
		MaxCount:  1,
	}, func(response *memdx.RangeScanDataResponse) error {
		assert.False(t, response.KeysOnly)
		items = append(items, response.Items...)
		return nil
	}, func(response *memdx.RangeScanActionResponse, err error) {
		waitAction <- memdx.UnaryResult[*memdx.RangeScanActionResponse]{
			Resp: response,
			Err:  err,
		}
	})
	require.NoError(t, err)

	action := <-waitAction
	require.NoError(t, action.Err)

	assert.True(t, action.Resp.More)
	assert.False(t, action.Resp.Complete)

	assert.Len(t, items, 1)
	item := items[0]

	assert.Contains(t, string(item.Key), baseKey)
	assert.Equal(t, value, item.Value)
	assert.Equal(t, datatype, item.Datatype)
	assert.NotZero(t, item.Cas)
	assert.NotZero(t, item.SeqNo)
	assert.Zero(t, item.Expiry)
	assert.Zero(t, item.Flags)
}

func TestOpsCrudRangeScanCreateContinueCancel(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureRangeScan)

	baseKey := "s" + uuid.NewString()[:6]
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)
	t.Cleanup(func() {
		err := cli.Close()
		require.NoError(t, err)
	})

	var highSeqNo uint64
	var vbUUID uint64
	for i := 0; i < 10; i++ {
		resp, err := memdx.SyncUnaryCall(memdx.OpsCrud{
			CollectionsEnabled: true,
			ExtFramesEnabled:   true,
		}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
			CollectionID: 0,
			Key:          []byte(string(baseKey) + "-" + strconv.Itoa(i)),
			VbucketID:    defaultTestVbucketID,
			Value:        value,
			Datatype:     datatype,
		})
		require.NoError(t, err)

		if resp.MutationToken.SeqNo > highSeqNo {
			highSeqNo = resp.MutationToken.SeqNo
			vbUUID = resp.MutationToken.VbUuid
		}
	}

	createResp, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.RangeScanCreate, cli, &memdx.RangeScanCreateRequest{
		CollectionID: 0,
		VbucketID:    defaultTestVbucketID,
		Range: &memdx.RangeScanCreateRangeScanConfig{
			Start: []byte(baseKey),
			End:   append([]byte(baseKey), "-\xFF"...),
		},
		// Server is sometimes returning empty range if we don't set snapshot_requirements.
		Snapshot: &memdx.RangeScanCreateSnapshotRequirements{
			VbUUID:   vbUUID,
			SeqNo:    highSeqNo,
			Deadline: time.Now().Add(2500 * time.Millisecond),
		},
	})
	require.NoError(t, err)

	scanUUID := createResp.ScanUUUID
	assert.NotEmpty(t, scanUUID)

	var items []memdx.RangeScanItem
	waitAction := make(chan memdx.UnaryResult[*memdx.RangeScanActionResponse], 1)
	_, err = memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}.RangeScanContinue(cli, &memdx.RangeScanContinueRequest{
		VbucketID: defaultTestVbucketID,
		ScanUUID:  scanUUID,
		MaxCount:  1,
	}, func(response *memdx.RangeScanDataResponse) error {
		assert.False(t, response.KeysOnly)
		items = append(items, response.Items...)
		return nil
	}, func(response *memdx.RangeScanActionResponse, err error) {
		waitAction <- memdx.UnaryResult[*memdx.RangeScanActionResponse]{
			Resp: response,
			Err:  err,
		}
	})
	require.NoError(t, err)

	action := <-waitAction
	require.NoError(t, action.Err)

	assert.True(t, action.Resp.More)
	assert.False(t, action.Resp.Complete)

	_, err = memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.RangeScanCancel, cli, &memdx.RangeScanCancelRequest{
		ScanUUID:  scanUUID,
		VbucketID: defaultTestVbucketID,
	})
	require.NoError(t, err)
}
