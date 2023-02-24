package cbhttpx

import (
	"context"
	"net/http"
	"testing"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/require"
)

func getHttpMgmt() *HttpManagement {
	return &HttpManagement{
		HttpClient: &http.Client{},
		UserAgent:  "gocbcorex test",
		Endpoint:   "http://" + testutils.TestOpts.HTTPAddrs[0],
		Username:   testutils.TestOpts.Username,
		Password:   testutils.TestOpts.Password,
	}
}

func TestHttpMgmtTerseClusterConfig(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	resp, err := getHttpMgmt().GetTerseClusterConfig(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Greater(t, resp.Rev, 0)
}

func TestHttpMgmtCollectionManagement(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	ctx := context.Background()
	bucketName := testutils.TestOpts.BucketName
	testScopeName := "test-scope-" + testutils.RunName
	testCollectionName := "test-scope-" + testutils.RunName

	err := getHttpMgmt().CreateScope(ctx, bucketName, testScopeName)
	require.NoError(t, err)

	err = getHttpMgmt().CreateCollection(ctx, bucketName, testScopeName, testCollectionName, nil)
	require.NoError(t, err)

	listResp, err := getHttpMgmt().GetCollectionManifest(ctx, bucketName)
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

	err = getHttpMgmt().DeleteCollection(ctx, bucketName, testScopeName, testCollectionName)
	require.NoError(t, err)

	err = getHttpMgmt().DeleteScope(ctx, bucketName, testScopeName)
	require.NoError(t, err)
}
