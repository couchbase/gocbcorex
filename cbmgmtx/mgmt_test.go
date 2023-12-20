package cbmgmtx_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/require"
)

func getHttpMgmt() *cbmgmtx.Management {
	return &cbmgmtx.Management{
		Transport: nil,
		UserAgent: "gocbcorex test",
		Endpoint:  "http://" + testutils.TestOpts.HTTPAddrs[0],
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}
}

func TestHttpMgmtFullClusterConfig(t *testing.T) {
	testutils.SkipIfShortTest(t)

	resp, err := getHttpMgmt().GetClusterConfig(context.Background(), &cbmgmtx.GetClusterConfigOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotEmpty(t, resp.Name)
}

func TestHttpMgmtStreamFullClusterConfig(t *testing.T) {
	testutils.SkipIfShortTest(t)

	ctx, cancel := context.WithCancel(context.Background())

	resp, err := getHttpMgmt().StreamFullClusterConfig(ctx, &cbmgmtx.StreamFullClusterConfigOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp)

	res, err := resp.Recv()
	require.NoError(t, err)

	assert.NotEmpty(t, res.Name)

	cancel()
	res, err = resp.Recv()
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, res)
}

func TestHttpMgmtTerseClusterConfig(t *testing.T) {
	testutils.SkipIfShortTest(t)

	resp, err := getHttpMgmt().GetTerseClusterConfig(context.Background(), &cbmgmtx.GetTerseClusterConfigOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Greater(t, resp.Rev, 0)
}

func TestHttpMgmtCollections(t *testing.T) {
	testutils.SkipIfShortTest(t)

	ctx := context.Background()
	bucketName := testutils.TestOpts.BucketName
	testScopeName := "testscope-" + uuid.NewString()[:6]
	testCollectionName := "testscope-" + uuid.NewString()[:6]

	_, err := getHttpMgmt().CreateScope(ctx, &cbmgmtx.CreateScopeOptions{
		BucketName: bucketName,
		ScopeName:  testScopeName,
	})
	require.NoError(t, err)

	_, err = getHttpMgmt().CreateScope(ctx, &cbmgmtx.CreateScopeOptions{
		BucketName: bucketName,
		ScopeName:  testScopeName,
	})
	require.ErrorIs(t, err, cbmgmtx.ErrScopeExists)

	_, err = getHttpMgmt().CreateCollection(ctx, &cbmgmtx.CreateCollectionOptions{
		BucketName:     bucketName,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		MaxTTL:         0,
	})
	require.NoError(t, err)

	_, err = getHttpMgmt().CreateCollection(ctx, &cbmgmtx.CreateCollectionOptions{
		BucketName:     bucketName,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		MaxTTL:         0,
	})
	require.ErrorIs(t, err, cbmgmtx.ErrCollectionExists)

	listResp, err := getHttpMgmt().GetCollectionManifest(ctx, &cbmgmtx.GetCollectionManifestOptions{
		BucketName: bucketName,
	})
	require.NoError(t, err)
	require.NotEmpty(t, listResp.UID)

	var foundScope *cbmgmtx.CollectionManifestScopeJson
	for _, scope := range listResp.Scopes {
		if scope.Name == testScopeName {
			foundScope = &scope
			break
		}
	}
	require.NotNil(t, foundScope)
	require.NotEmpty(t, foundScope.UID)

	var foundCollection *cbmgmtx.CollectionManifestCollectionJson
	for _, collection := range foundScope.Collections {
		if collection.Name == testCollectionName {
			foundCollection = &collection
			break
		}
	}
	require.NotNil(t, foundCollection)
	require.NotEmpty(t, foundScope.UID)

	_, err = getHttpMgmt().DeleteCollection(ctx, &cbmgmtx.DeleteCollectionOptions{
		BucketName:     bucketName,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
	})
	require.NoError(t, err)

	_, err = getHttpMgmt().DeleteCollection(ctx, &cbmgmtx.DeleteCollectionOptions{
		BucketName:     bucketName,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
	})
	require.ErrorIs(t, err, cbmgmtx.ErrCollectionNotFound)

	_, err = getHttpMgmt().DeleteScope(ctx, &cbmgmtx.DeleteScopeOptions{
		BucketName: bucketName,
		ScopeName:  testScopeName,
	})
	require.NoError(t, err)

	_, err = getHttpMgmt().DeleteScope(ctx, &cbmgmtx.DeleteScopeOptions{
		BucketName: bucketName,
		ScopeName:  testScopeName,
	})
	require.ErrorIs(t, err, cbmgmtx.ErrScopeNotFound)
}

func TestHttpMgmtBuckets(t *testing.T) {
	testutils.SkipIfShortTest(t)

	ctx := context.Background()
	testBucketName := "testbucket-" + uuid.NewString()[:6]

	bucketSettings := cbmgmtx.BucketSettings{
		MutableBucketSettings: cbmgmtx.MutableBucketSettings{
			RAMQuotaMB:         128,
			EvictionPolicy:     cbmgmtx.EvictionPolicyTypeValueOnly,
			CompressionMode:    cbmgmtx.CompressionModePassive,
			DurabilityMinLevel: cbmgmtx.DurabilityLevelNone,
		},
		ConflictResolutionType: cbmgmtx.ConflictResolutionTypeSequenceNumber,
		BucketType:             cbmgmtx.BucketTypeCouchbase,
		StorageBackend:         cbmgmtx.StorageBackendCouchstore,
		ReplicaIndex:           true,
	}

	err := getHttpMgmt().CreateBucket(ctx, &cbmgmtx.CreateBucketOptions{
		BucketName:     testBucketName,
		BucketSettings: bucketSettings,
	})
	require.NoError(t, err)

	// BUG(ING-685): We have to wait for the bucket to appear.
	var returendDef *cbmgmtx.BucketDef
	require.Eventually(t, func() bool {
		def, err := getHttpMgmt().GetBucket(ctx, &cbmgmtx.GetBucketOptions{
			BucketName: testBucketName,
		})
		if err != nil {
			return false
		}

		returendDef = def
		return true
	}, 30*time.Second, 100*time.Millisecond)
	require.Equal(t, bucketSettings, returendDef.BucketSettings)

	updatedSettings := bucketSettings.MutableBucketSettings
	updatedSettings.FlushEnabled = true
	err = getHttpMgmt().UpdateBucket(ctx, &cbmgmtx.UpdateBucketOptions{
		BucketName:            testBucketName,
		MutableBucketSettings: updatedSettings,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		err = getHttpMgmt().FlushBucket(ctx, &cbmgmtx.FlushBucketOptions{
			BucketName: testBucketName,
		})
		return err == nil
	}, 30*time.Second, 100*time.Millisecond)

	err = getHttpMgmt().DeleteBucket(ctx, &cbmgmtx.DeleteBucketOptions{
		BucketName: testBucketName,
	})
	require.NoError(t, err)

	err = getHttpMgmt().CreateBucket(ctx, &cbmgmtx.CreateBucketOptions{
		BucketName:     testutils.TestOpts.BucketName,
		BucketSettings: bucketSettings,
	})
	require.ErrorIs(t, err, cbmgmtx.ErrBucketExists)

	err = getHttpMgmt().UpdateBucket(ctx, &cbmgmtx.UpdateBucketOptions{
		BucketName:            "missing-bucket-name",
		MutableBucketSettings: updatedSettings,
	})
	require.ErrorIs(t, err, cbmgmtx.ErrBucketNotFound)

	err = getHttpMgmt().FlushBucket(ctx, &cbmgmtx.FlushBucketOptions{
		BucketName: "missing-bucket-name",
	})
	require.ErrorIs(t, err, cbmgmtx.ErrBucketNotFound)

	err = getHttpMgmt().DeleteBucket(ctx, &cbmgmtx.DeleteBucketOptions{
		BucketName: "missing-bucket-name",
	})
	require.ErrorIs(t, err, cbmgmtx.ErrBucketNotFound)
}

func Test_parseForInvalidArg(t *testing.T) {
	errTextStart := `{"errors":{`
	errTextEnd := `},"summaries":{"ramSummary":{"total":3028287488,"otherBuckets":0,
	"nodesCount":1,"perNodeMegs":100,"thisAlloc":104857600,"thisUsed":0,"free":2923429888},
	"hddSummary":{"total":63089455104,"otherData":5047156408,"otherBuckets":0,"thisUsed":0,
	"free":58042298696}}}`

	t.Run("single field in chain", func(t *testing.T) {
		errText := errTextStart + `"durability_min_level":"reasonOne"` + errTextEnd

		resp := &http.Response{
			StatusCode: 400,
			Body:       io.NopCloser(strings.NewReader(errText)),
		}

		err := cbmgmtx.Management{}.DecodeCommonError(resp)
		var sErr *cbmgmtx.ServerInvalidArgError
		require.ErrorAs(t, err, &sErr)

		assert.Equal(t, "DurabilityMinLevel", sErr.Argument)
		assert.Equal(t, "reasonone", sErr.Reason)
	})

	t.Run("multiple fields in chain", func(t *testing.T) {
		errText := errTextStart + `"durability_min_level":"reasonOne"` + errTextEnd

		resp := &http.Response{
			StatusCode: 400,
			Body:       io.NopCloser(strings.NewReader(errText)),
		}

		err := cbmgmtx.Management{}.DecodeCommonError(resp)
		var sErr *cbmgmtx.ServerInvalidArgError
		require.ErrorAs(t, err, &sErr)

		isFirstError := sErr.Argument == "DurabilityMinLevel"
		if isFirstError {
			assert.Equal(t, sErr.Reason, "reasonone")
		} else {
			assert.Equal(t, sErr.Argument, "RamQuotaMB")
			assert.Equal(t, sErr.Reason, "reasontwo")
		}
	})

	t.Run("single field in chain - commas in reason", func(t *testing.T) {
		errText := errTextStart + `"replicaNumber":"reasonOne, something else"` + errTextEnd
		resp := &http.Response{
			StatusCode: 400,
			Body:       io.NopCloser(strings.NewReader(errText)),
		}

		err := cbmgmtx.Management{}.DecodeCommonError(resp)
		var sErr *cbmgmtx.ServerInvalidArgError
		require.ErrorAs(t, err, &sErr)

		assert.Equal(t, "ReplicaNumber", sErr.Argument)
		assert.Equal(t, "reasonone, something else", sErr.Reason)
	})

	t.Run("multiple fields in chain - commas in reasons", func(t *testing.T) {
		errText := errTextStart + `"durability_min_level":"reasonOne, something else","ramquota":"reason, something"` + errTextEnd
		resp := &http.Response{
			StatusCode: 400,
			Body:       io.NopCloser(strings.NewReader(errText)),
		}

		err := cbmgmtx.Management{}.DecodeCommonError(resp)
		var sErr *cbmgmtx.ServerInvalidArgError
		require.ErrorAs(t, err, &sErr)

		isFirstError := sErr.Argument == "DurabilityMinLevel"
		if isFirstError {
			assert.Equal(t, sErr.Reason, "reasonone, something else")
		} else {
			assert.Equal(t, sErr.Argument, "RamQuotaMB")
			assert.Equal(t, sErr.Reason, "reason, something")
		}
	})
}
