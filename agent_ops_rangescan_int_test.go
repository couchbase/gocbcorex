package gocbcorex_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/leakcheck"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRangeScanRangeLargeValues(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureRangeScan)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
		require.False(t, leakcheck.ReportLeakedGoroutines())
	})

	size := 8192 * 2
	value := make([]byte, size)
	for i := 0; i < size; i++ {
		value[i] = byte(i)
	}

	docIDs := []string{"largevalues-2960", "largevalues-3064", "largevalues-3686", "largevalues-3716", "largevalues-5354",
		"largevalues-5426", "largevalues-6175", "largevalues-6607", "largevalues-6797", "largevalues-7871"}
	muts := setupRangeScan(t, agent, docIDs, value, "", "")

	data := doRangeScan(t, agent,
		&gocbcorex.RangeScanCreateOptions{
			Range: &memdx.RangeScanCreateRangeScanConfig{
				Start: []byte("largevalues"),
				End:   []byte("largevalues\xFF"),
			},
			Snapshot: &memdx.RangeScanCreateSnapshotRequirements{
				VbUUID: muts.vbuuid,
				SeqNo:  muts.highSeqNo,
			},
			ScopeName:      "",
			CollectionName: "",
			VbucketID:      12,
		},
		&gocbcorex.RangeScanContinueOptions{},
	)

	itemsMap := make(map[string]memdx.RangeScanItem)
	for _, item := range data {
		itemsMap[string(item.Key)] = item
	}

	for id, mut := range muts.muts {
		item, ok := itemsMap[id]
		if assert.True(t, ok) {
			assert.Equal(t, mut.cas, item.Cas)
			assert.Equal(t, mut.mutationToken.SeqNo, item.SeqNo)
			// If we just use assert.Equal here then the test log will get spammed in fail cases.
			assert.True(t, bytes.Equal(value, item.Value), "Actual value not expected")
		}
	}
}

func TestRangeScanRangeSmallValues(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureRangeScan)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
		require.False(t, leakcheck.ReportLeakedGoroutines())
	})

	value := []byte(`{"barry": "sheen"}`)

	docIDs := []string{"rangesmallvalues-1023", "rangesmallvalues-1751", "rangesmallvalues-2202",
		"rangesmallvalues-2392", "rangesmallvalues-2570", "rangesmallvalues-4132", "rangesmallvalues-4640",
		"rangesmallvalues-5836", "rangesmallvalues-7283", "rangesmallvalues-7313"}
	muts := setupRangeScan(t, agent, docIDs, value, "", "")

	data := doRangeScan(t, agent,
		&gocbcorex.RangeScanCreateOptions{
			Range: &memdx.RangeScanCreateRangeScanConfig{
				Start: []byte("rangesmallvalues"),
				End:   []byte("rangesmallvalues\xFF"),
			},
			Snapshot: &memdx.RangeScanCreateSnapshotRequirements{
				VbUUID: muts.vbuuid,
				SeqNo:  muts.highSeqNo,
			},
			ScopeName:      "",
			CollectionName: "",
			VbucketID:      12,
		},
		&gocbcorex.RangeScanContinueOptions{},
	)

	itemsMap := make(map[string]memdx.RangeScanItem)
	for _, item := range data {
		itemsMap[string(item.Key)] = item
	}

	for id, mut := range muts.muts {
		item, ok := itemsMap[id]
		if assert.True(t, ok) {
			assert.Equal(t, mut.cas, item.Cas)
			assert.Equal(t, mut.mutationToken.SeqNo, item.SeqNo)
			// If we just use assert.Equal here then the test log will get spammed in fail cases.
			assert.True(t, bytes.Equal(value, item.Value), "Actual value not expected")
		}
	}
}

func TestRangeScanRangeKeysOnly(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureRangeScan)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
		require.False(t, leakcheck.ReportLeakedGoroutines())
	})

	value := "value"
	docIDs := []string{"rangekeysonly-1269", "rangekeysonly-2048", "rangekeysonly-4378", "rangekeysonly-7159",
		"rangekeysonly-8898", "rangekeysonly-8908", "rangekeysonly-19559", "rangekeysonly-20808",
		"rangekeysonly-20998", "rangekeysonly-25889"}
	muts := setupRangeScan(t, agent, docIDs, []byte(value), "", "")

	data := doRangeScan(t, agent,
		&gocbcorex.RangeScanCreateOptions{
			Range: &memdx.RangeScanCreateRangeScanConfig{
				Start: []byte("rangekeysonly"),
				End:   []byte("rangekeysonly\xFF"),
			},
			Snapshot: &memdx.RangeScanCreateSnapshotRequirements{
				VbUUID: muts.vbuuid,
				SeqNo:  muts.highSeqNo,
			},
			KeysOnly:  true,
			VbucketID: 12,
		},
		&gocbcorex.RangeScanContinueOptions{},
	)

	itemsMap := make(map[string]memdx.RangeScanItem)
	for _, item := range data {
		itemsMap[string(item.Key)] = item
	}

	for id := range muts.muts {
		item, ok := itemsMap[id]
		if assert.True(t, ok) {
			assert.Zero(t, item.Cas)
			assert.Zero(t, item.SeqNo)
			assert.Empty(t, item.Value)
		}
	}
}

func TestRangeScanSamplingKeysOnly(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureRangeScan)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
		require.False(t, leakcheck.ReportLeakedGoroutines())
	})

	scopeName := "sample" + uuid.NewString()[:6]
	collectionName := "sample" + uuid.NewString()[:6]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	CreateAndEnsureScope(ctx, t, agent, testutilsint.TestOpts.BucketName, scopeName)
	t.Cleanup(func() {
		_, err := agent.DeleteScope(context.Background(), &cbmgmtx.DeleteScopeOptions{
			BucketName: testutilsint.TestOpts.BucketName,
			ScopeName:  scopeName,
		})
		require.NoError(t, err)
	})

	CreateAndEnsureCollection(ctx, t, agent, testutilsint.TestOpts.BucketName, scopeName, collectionName)
	t.Cleanup(func() {
		_, err := agent.DeleteCollection(context.Background(), &cbmgmtx.DeleteCollectionOptions{
			BucketName:     testutilsint.TestOpts.BucketName,
			ScopeName:      scopeName,
			CollectionName: collectionName,
		})
		require.NoError(t, err)
	})

	value := "value"
	docIDs := []string{"samplescankeys-170", "samplescankeys-602", "samplescankeys-792", "samplescankeys-3978",
		"samplescankeys-6869", "samplescankeys-9038", "samplescankeys-10806", "samplescankeys-10996",
		"samplescankeys-11092", "samplescankeys-11102"}
	muts := setupRangeScan(t, agent, docIDs, []byte(value), collectionName, scopeName)

	data := doRangeScan(t, agent,
		&gocbcorex.RangeScanCreateOptions{
			Sampling: &memdx.RangeScanCreateRandomSamplingConfig{
				Samples: 10,
			},
			Snapshot: &memdx.RangeScanCreateSnapshotRequirements{
				VbUUID: muts.vbuuid,
				SeqNo:  muts.highSeqNo,
			},
			KeysOnly:       true,
			ScopeName:      scopeName,
			CollectionName: collectionName,
			VbucketID:      12,
		},
		&gocbcorex.RangeScanContinueOptions{},
	)

	itemsMap := make(map[string]memdx.RangeScanItem)
	for _, item := range data {
		itemsMap[string(item.Key)] = item
	}

	for id := range muts.muts {
		item, ok := itemsMap[id]
		if assert.True(t, ok) {
			assert.Zero(t, item.Cas)
			assert.Zero(t, item.SeqNo)
			assert.Empty(t, item.Value)
		}
	}
}

func TestRangeScanRangeCancellation(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureRangeScan)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
		require.False(t, leakcheck.ReportLeakedGoroutines())
	})

	value := "value"
	docIDs := []string{"rangescancancel-2746", "rangescancancel-37795", "rangescancancel-63440", "rangescancancel-116036",
		"rangescancancel-136879", "rangescancancel-156589", "rangescancancel-196316", "rangescancancel-203197",
		"rangescancancel-243428", "rangescancancel-257242"}

	muts := setupRangeScan(t, agent, docIDs, []byte(value), "", "")

	res, err := agent.RangeScanCreate(context.Background(), &gocbcorex.RangeScanCreateOptions{
		Range: &memdx.RangeScanCreateRangeScanConfig{
			Start: []byte("rangescancancel"),
			End:   []byte("rangescancancel\xFF"),
		},
		Snapshot: &memdx.RangeScanCreateSnapshotRequirements{
			VbUUID: muts.vbuuid,
			SeqNo:  muts.highSeqNo,
		},
		VbucketID: 12,
	})
	require.NoError(t, err)

	_, err = res.Cancel(context.Background(), &gocbcorex.RangeScanCancelOptions{})
	require.NoError(t, err)
}

func TestRangeScanRangeContinueClosedClient(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureRangeScan)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
		require.False(t, leakcheck.ReportLeakedGoroutines())
	})

	value := "value"
	docIDs := []string{"rangescancancel-2746", "rangescancancel-37795", "rangescancancel-63440", "rangescancancel-116036",
		"rangescancancel-136879", "rangescancancel-156589", "rangescancancel-196316", "rangescancancel-203197",
		"rangescancancel-243428", "rangescancancel-257242"}

	muts := setupRangeScan(t, agent, docIDs, []byte(value), "", "")

	res, err := agent.RangeScanCreate(context.Background(), &gocbcorex.RangeScanCreateOptions{
		Range: &memdx.RangeScanCreateRangeScanConfig{
			Start: []byte("rangescancancel"),
			End:   []byte("rangescancancel\xFF"),
		},
		Snapshot: &memdx.RangeScanCreateSnapshotRequirements{
			VbUUID: muts.vbuuid,
			SeqNo:  muts.highSeqNo,
		},
		VbucketID: 12,
	})
	require.NoError(t, err)

	err = agent.Close()
	require.NoError(t, err)

	_, err = res.Continue(context.Background(), &gocbcorex.RangeScanContinueOptions{}, func(result gocbcorex.RangeScanContinueDataResult) {})
	require.Error(t, err)
}

func TestRangeScanRangeCancelClosedClient(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureRangeScan)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
		require.False(t, leakcheck.ReportLeakedGoroutines())
	})

	value := "value"
	docIDs := []string{"rangescancancel-2746", "rangescancancel-37795", "rangescancancel-63440", "rangescancancel-116036",
		"rangescancancel-136879", "rangescancancel-156589", "rangescancancel-196316", "rangescancancel-203197",
		"rangescancancel-243428", "rangescancancel-257242"}

	muts := setupRangeScan(t, agent, docIDs, []byte(value), "", "")

	res, err := agent.RangeScanCreate(context.Background(), &gocbcorex.RangeScanCreateOptions{
		Range: &memdx.RangeScanCreateRangeScanConfig{
			Start: []byte("rangescancancel"),
			End:   []byte("rangescancancel\xFF"),
		},
		Snapshot: &memdx.RangeScanCreateSnapshotRequirements{
			VbUUID: muts.vbuuid,
			SeqNo:  muts.highSeqNo,
		},
		VbucketID: 12,
	})
	require.NoError(t, err)

	err = agent.Close()
	require.NoError(t, err)

	_, err = res.Cancel(context.Background(), &gocbcorex.RangeScanCancelOptions{})
	require.Error(t, err)
}

type rangeScanMutation struct {
	cas           uint64
	mutationToken gocbcorex.MutationToken
}

type rangeScanMutations struct {
	muts      map[string]rangeScanMutation
	vbuuid    uint64
	highSeqNo uint64
}

func setupRangeScan(t *testing.T, agent *gocbcorex.Agent, docIDs []string, value []byte, collection, scope string) *rangeScanMutations {
	muts := &rangeScanMutations{
		muts: make(map[string]rangeScanMutation),
	}
	for i := 0; i < len(docIDs); i++ {
		res, err := agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
			Key:            []byte(docIDs[i]),
			Value:          value,
			ScopeName:      scope,
			CollectionName: collection,
		})
		require.NoError(t, err)
		muts.muts[docIDs[i]] = rangeScanMutation{
			cas:           res.Cas,
			mutationToken: res.MutationToken,
		}

		if res.MutationToken.SeqNo > muts.highSeqNo {
			muts.highSeqNo = res.MutationToken.SeqNo
			muts.vbuuid = res.MutationToken.VbUuid
		}
	}

	return muts
}

func doRangeScan(t *testing.T, agent *gocbcorex.Agent, opts *gocbcorex.RangeScanCreateOptions,
	contOpts *gocbcorex.RangeScanContinueOptions) []memdx.RangeScanItem {

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cRes, err := agent.RangeScanCreate(ctx, opts)
	require.NoError(t, err)

	var data []memdx.RangeScanItem
	for {
		res, err := cRes.Continue(ctx, contOpts, func(result gocbcorex.RangeScanContinueDataResult) {
			data = append(data, result.Items...)
		})
		require.NoError(t, err)

		if res.More {
			continue
		}

		if res.Complete {
			return data
		}
	}
}
