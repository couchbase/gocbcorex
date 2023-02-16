package memdx

import (
	"encoding/json"
	"strconv"
	"testing"

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

	createValue := make(map[string]interface{})
	createValue["start"] = key
	createValue["end"] = key

	// Server is sometimes returning empty range if we don't set snapshot_requirements.
	createValue["snapshot_requirements"] = map[string]interface{}{
		"vb_uuid":    strconv.FormatUint(resp.MutationToken.VbUuid, 10),
		"seqno":      resp.MutationToken.SeqNo,
		"timeout_ms": 2500,
	}

	createBytes, err := json.Marshal(createValue)
	require.NoError(t, err)

	createResp, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.RangeScanCreate, cli, &RangeScanCreateRequest{
		CollectionID: 0,
		VbucketID:    1,
		Value:        createBytes,
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
	// A bit gross, but effective.
	assert.Contains(t, string(continueResp.Value), string(key))
	assert.Contains(t, string(continueResp.Value), string(value))
}

// Failing because of what looks like a server issue.
// func TestOpsCrudRangeScanCreateContinueMoreThanOnce(t *testing.T) {
// 	if !testutils.TestOpts.LongTest {
// 		t.SkipNow()
// 	}
//
// 	baseKey := "s" + uuid.NewString()[:6]
// 	value := []byte("{\"key\": \"value\"}")
// 	datatype := uint8(0x01)
//
// 	cli := createTestClient(t)
// 	defer cli.Close()
//
// 	var highSeqNo uint64
// 	var vbUUID uint64
// 	for i := 0; i < 10; i++ {
// 		resp, err := syncUnaryCall(OpsCrud{
// 			CollectionsEnabled: true,
// 			ExtFramesEnabled:   true,
// 		}, OpsCrud.Set, cli, &SetRequest{
// 			CollectionID: 0,
// 			Key:          []byte(string(baseKey) + "-" + strconv.Itoa(i)),
// 			VbucketID:    1,
// 			Value:        value,
// 			Datatype:     datatype,
// 		})
// 		require.NoError(t, err)
//
// 		if resp.MutationToken.SeqNo > highSeqNo {
// 			highSeqNo = resp.MutationToken.SeqNo
// 			vbUUID = resp.MutationToken.VbUuid
// 		}
// 	}
//
// 	fmt.Println("key:" + string(baseKey))
// 	fmt.Println("start:" + string(baseKey))
// 	fmt.Println("end:" + string(baseKey) + "-\xFF")
//
// 	createValue := make(map[string]interface{})
// 	createValue["start"] = string(baseKey)
// 	createValue["end"] = string(baseKey) + "-10"
//
// 	// Server is sometimes returning empty range if we don't set snapshot_requirements.
// 	createValue["snapshot_requirements"] = map[string]interface{}{
// 		"vb_uuid":    strconv.FormatUint(vbUUID, 10),
// 		"seqno":      highSeqNo,
// 		"timeout_ms": 2500,
// 	}
//
// 	createBytes, err := json.Marshal(createValue)
// 	require.NoError(t, err)
//
// 	createResp, err := syncUnaryCall(OpsCrud{
// 		CollectionsEnabled: true,
// 		ExtFramesEnabled:   true,
// 	}, OpsCrud.RangeScanCreate, cli, &RangeScanCreateRequest{
// 		CollectionID: 0,
// 		VbucketID:    1,
// 		Value:        createBytes,
// 	})
// 	require.NoError(t, err)
//
// 	scanUUID := createResp.ScanUUUID
// 	assert.NotEmpty(t, scanUUID)
//
// 	continueResp, err := syncUnaryCall(OpsCrud{
// 		CollectionsEnabled: true,
// 		ExtFramesEnabled:   true,
// 	}, OpsCrud.RangeScanContinue, cli, &RangeScanContinueRequest{
// 		VbucketID: 1,
// 		ScanUUID:  scanUUID,
// 		MaxCount:  1,
// 	})
// 	require.NoError(t, err)
//
// 	assert.False(t, continueResp.KeysOnly)
// 	assert.True(t, continueResp.More)
// 	assert.False(t, continueResp.Complete)
// 	// A bit gross, but effective.
// 	assert.Contains(t, string(continueResp.Value), string(baseKey))
// 	assert.Contains(t, string(continueResp.Value), string(value))
// }

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

	createValue := make(map[string]interface{})
	createValue["start"] = baseKey
	createValue["end"] = baseKey + "-10"

	// Server is sometimes returning empty range if we don't set snapshot_requirements.
	createValue["snapshot_requirements"] = map[string]interface{}{
		"vb_uuid":    strconv.FormatUint(vbUUID, 10),
		"seqno":      highSeqNo,
		"timeout_ms": 2500,
	}

	createBytes, err := json.Marshal(createValue)
	require.NoError(t, err)

	createResp, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.RangeScanCreate, cli, &RangeScanCreateRequest{
		CollectionID: 0,
		VbucketID:    1,
		Value:        createBytes,
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
