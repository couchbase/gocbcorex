package cbmgmtx_test

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func getHttpMgmt() *cbmgmtx.Management {
	return &cbmgmtx.Management{
		Transport: nil,
		UserAgent: "gocbcorex test",
		Endpoint:  "http://" + testutilsint.TestOpts.HTTPAddrs[0],
		Username:  testutilsint.TestOpts.Username,
		Password:  testutilsint.TestOpts.Password,
	}
}

func TestHttpMgmtFullClusterConfig(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	resp, err := getHttpMgmt().GetClusterConfig(context.Background(), &cbmgmtx.GetClusterConfigOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotEmpty(t, resp.Name)
}

func TestHttpMgmtStreamFullClusterConfig(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

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
	testutilsint.SkipIfShortTest(t)

	resp, err := getHttpMgmt().GetTerseClusterConfig(context.Background(), &cbmgmtx.GetTerseClusterConfigOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Greater(t, resp.Rev, 0)
}

func TestHttpMgmtCollections(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()
	bucketName := testutilsint.TestOpts.BucketName
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
	testutilsint.SkipIfShortTest(t)

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
		BucketName:     testutilsint.TestOpts.BucketName,
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

func TestHttpMgmtAutoFailover(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()

	settings, err := getHttpMgmt().GetAutoFailoverSettings(ctx, &cbmgmtx.GetAutoFailoverSettingsRequest{})
	require.NoError(t, err)

	err = getHttpMgmt().ConfigureAutoFailover(ctx, &cbmgmtx.ConfigureAutoFailoverRequest{
		Enabled: ptr.To(settings.Enabled),
		Timeout: ptr.To(settings.Timeout),
	})
	require.NoError(t, err)
}

func TestHttpMgmtUsers(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()
	testUsername := "testuser-" + uuid.NewString()[:6]

	err := getHttpMgmt().UpsertUser(ctx, &cbmgmtx.UpsertUserOptions{
		Username:    testUsername,
		DisplayName: testUsername,
		Password:    "password",
		Roles:       []string{"ro_admin"},
	})
	require.NoError(t, err)

	users, err := getHttpMgmt().GetAllUsers(ctx, &cbmgmtx.GetAllUsersOptions{})
	require.NoError(t, err)

	userIdx := slices.IndexFunc(users, func(user *cbmgmtx.UserJson) bool {
		return user.Name == testUsername
	})
	require.GreaterOrEqual(t, userIdx, 0)
	assert.Equal(t, users[userIdx].Name, testUsername)
	assert.Equal(t, users[userIdx].Domain, cbmgmtx.AuthDomainLocal)

	err = getHttpMgmt().DeleteUser(ctx, &cbmgmtx.DeleteUserOptions{
		Username: testUsername,
	})
	require.NoError(t, err)

	err = getHttpMgmt().DeleteUser(ctx, &cbmgmtx.DeleteUserOptions{
		Username: "missing-user-name",
	})
	require.ErrorIs(t, err, cbmgmtx.ErrUserNotFound)
}
