package cbmgmtx

import (
	"context"
	"testing"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/require"
)

func getHttpMgmt() *Management {
	return &Management{
		Transport: nil,
		UserAgent: "gocbcorex test",
		Endpoint:  "http://" + testutils.TestOpts.HTTPAddrs[0],
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}
}

func TestHttpMgmtTerseClusterConfig(t *testing.T) {
	testutils.SkipIfShortTest(t)

	resp, err := getHttpMgmt().GetTerseClusterConfig(context.Background(), &GetTerseClusterConfigOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Greater(t, resp.Rev, 0)
}

func TestHttpMgmtCollectionManagement(t *testing.T) {
	testutils.SkipIfShortTest(t)

	ctx := context.Background()
	bucketName := testutils.TestOpts.BucketName
	testScopeName := "testscope-" + testutils.TestOpts.RunName
	testCollectionName := "testscope-" + testutils.TestOpts.RunName

	err := getHttpMgmt().CreateScope(ctx, &CreateScopeOptions{
		BucketName: bucketName,
		ScopeName:  testScopeName,
	})
	require.NoError(t, err)

	err = getHttpMgmt().CreateCollection(ctx, &CreateCollectionOptions{
		BucketName:     bucketName,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		MaxTTL:         0,
	})
	require.NoError(t, err)

	listResp, err := getHttpMgmt().GetCollectionManifest(ctx, &GetCollectionManifestOptions{
		BucketName: bucketName,
	})
	require.NoError(t, err)
	require.NotEmpty(t, listResp.UID)

	var foundScope *CollectionManifestScopeJson
	for _, scope := range listResp.Scopes {
		if scope.Name == testScopeName {
			foundScope = &scope
			break
		}
	}
	require.NotNil(t, foundScope)
	require.NotEmpty(t, foundScope.UID)

	var foundCollection *CollectionManifestCollectionJson
	for _, collection := range foundScope.Collections {
		if collection.Name == testCollectionName {
			foundCollection = &collection
			break
		}
	}
	require.NotNil(t, foundCollection)
	require.NotEmpty(t, foundScope.UID)

	err = getHttpMgmt().DeleteCollection(ctx, &DeleteCollectionOptions{
		BucketName:     bucketName,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
	})
	require.NoError(t, err)

	err = getHttpMgmt().DeleteScope(ctx, &DeleteScopeOptions{
		BucketName: bucketName,
		ScopeName:  testScopeName,
	})
	require.NoError(t, err)
}

func TestHttpMgmtCollectionBuckets(t *testing.T) {
	testutils.SkipIfShortTest(t)

	ctx := context.Background()
	testBucketName := "testbucket-" + testutils.TestOpts.RunName

	bucketSettings := BucketSettings{
		MutableBucketSettings: MutableBucketSettings{
			RAMQuotaMB:         128,
			BucketType:         BucketTypeCouchbase,
			EvictionPolicy:     EvictionPolicyTypeValueOnly,
			CompressionMode:    CompressionModePassive,
			DurabilityMinLevel: DurabilityLevelNone,
			StorageBackend:     StorageBackendCouchstore,
		},
		ConflictResolutionType: ConflictResolutionTypeSequenceNumber,
	}

	err := getHttpMgmt().CreateBucket(ctx, &CreateBucketOptions{
		BucketName:     testBucketName,
		BucketSettings: bucketSettings,
	})
	require.NoError(t, err)

	def, err := getHttpMgmt().GetBucket(ctx, &GetBucketOptions{
		BucketName: testBucketName,
	})
	require.NoError(t, err)
	require.Equal(t, bucketSettings, def.BucketSettings)

	updatedSettings := def.MutableBucketSettings
	updatedSettings.FlushEnabled = true
	err = getHttpMgmt().UpdateBucket(ctx, &UpdateBucketOptions{
		BucketName:            testBucketName,
		MutableBucketSettings: updatedSettings,
	})
	require.NoError(t, err)

	// this can fail intermittently, so we retry up to 10 times
	for i := 0; i < 10; i++ {
		err = getHttpMgmt().FlushBucket(ctx, &FlushBucketOptions{
			BucketName: testBucketName,
		})
		if err == nil {
			break
		}
		t.Logf("warning: had to retry flushing bucket...")
	}
	require.NoError(t, err)

	err = getHttpMgmt().DeleteBucket(ctx, &DeleteBucketOptions{
		BucketName: testBucketName,
	})
	require.NoError(t, err)

}
