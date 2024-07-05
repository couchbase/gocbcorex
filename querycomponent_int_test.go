package gocbcorex_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/require"
)

type n1qlTestHelper struct {
	TestName      string
	NumDocs       int
	QueryTestDocs *testDocs
	Agent         *gocbcorex.Agent
	QueryFn       func(context.Context, *gocbcorex.QueryOptions) (gocbcorex.QueryResultStream, error)
	T             *testing.T
}

func hlpEnsurePrimaryIndex(t *testing.T, agent *gocbcorex.Agent, bucketName string) {
	t.Helper()

	err := agent.CreatePrimaryIndex(context.Background(), &cbqueryx.CreatePrimaryIndexOptions{
		BucketName:     bucketName,
		IgnoreIfExists: true,
	})
	require.NoError(t, err)
}

func (nqh *n1qlTestHelper) testSetupN1ql(t *testing.T) {
	nqh.QueryTestDocs = makeTestDocs(context.Background(), t, nqh.Agent, nqh.TestName, nqh.NumDocs)

	hlpEnsurePrimaryIndex(t, nqh.Agent, testutilsint.TestOpts.BucketName)
}

func (nqh *n1qlTestHelper) testCleanupN1ql(t *testing.T) {
	if nqh.QueryTestDocs != nil {
		nqh.QueryTestDocs.Remove(context.Background())
		nqh.QueryTestDocs = nil
	}
}

func (nqh *n1qlTestHelper) testN1QLBasic(t *testing.T) {
	deadline := time.Now().Add(30000 * time.Millisecond)
	runTestQuery := func() ([]testDoc, error) {
		iterDeadline := time.Now().Add(10000 * time.Millisecond)
		if iterDeadline.After(deadline) {
			iterDeadline = deadline
		}
		ctx, cancel := context.WithDeadline(context.Background(), iterDeadline)
		defer cancel()

		rows, err := nqh.QueryFn(ctx, &gocbcorex.QueryOptions{
			QueryOptions: cbqueryx.QueryOptions{
				ClientContextId: "12345",
				Statement:       fmt.Sprintf("SELECT i,testName FROM %s WHERE testName=\"%s\"", testutilsint.TestOpts.BucketName, nqh.TestName),
			},
		})
		if err != nil {
			nqh.T.Logf("Received error from query: %v", err)
			return nil, err
		}

		var docs []testDoc
		for {
			row, err := rows.ReadRow()
			if err != nil {
				return nil, err
			}

			if row == nil {
				nqh.T.Logf("Received now rows from query")
				break
			}

			var doc testDoc
			err = json.Unmarshal(row, &doc)
			if err != nil {
				nqh.T.Logf("Failed to unmarshal into testDoc: %v", err)
				return nil, err
			}

			docs = append(docs, doc)
		}

		return docs, nil
	}

	lastError := ""
	for {
		docs, err := runTestQuery()
		if err == nil {
			testFailed := false

			for _, doc := range docs {
				if doc.I < 1 || doc.I > nqh.NumDocs {
					lastError = fmt.Sprintf("query test read invalid row i=%d", doc.I)
					testFailed = true
				}
			}

			numDocs := len(docs)
			if numDocs != nqh.NumDocs {
				nqh.T.Logf("Received incorrect number of rows. Expected: %d, received: %d", nqh.NumDocs, numDocs)
				lastError = fmt.Sprintf("query test read invalid number of rows %d!=%d", numDocs, 5)
				testFailed = true
			}

			if !testFailed {
				break
			}
		}

		sleepDeadline := time.Now().Add(1000 * time.Millisecond)
		if sleepDeadline.After(deadline) {
			sleepDeadline = deadline
		}
		time.Sleep(time.Until(sleepDeadline))

		if sleepDeadline == deadline {
			t.Errorf("timed out waiting for indexing: %s", lastError)
			break
		}
	}
}

func TestQueryBasic(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	helper := &n1qlTestHelper{
		TestName: "testQuery",
		NumDocs:  5,
		Agent:    agent,
		QueryFn:  agent.Query,
		T:        t,
	}

	t.Run("setup", helper.testSetupN1ql)

	t.Run("Basic", helper.testN1QLBasic)

	t.Run("cleanup", helper.testCleanupN1ql)
}

func TestQueryPrepared(t *testing.T) {
	t.Skip()
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	helper := &n1qlTestHelper{
		TestName: "testQuery",
		NumDocs:  5,
		Agent:    agent,
		QueryFn:  agent.PreparedQuery,
		T:        t,
	}

	t.Run("setup", helper.testSetupN1ql)

	t.Run("Basic", helper.testN1QLBasic)

	t.Run("cleanup", helper.testCleanupN1ql)
}

func TestEnsureQueryIndex(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	idxName := uuid.NewString()[:6]
	err := agent.CreateIndex(ctx, &cbqueryx.CreateIndexOptions{
		BucketName:     testutilsint.TestOpts.BucketName,
		ScopeName:      "_default",
		CollectionName: "_default",
		IndexName:      idxName,
		Fields:         []string{"test"},
		NumReplicas:    nil,
		Deferred:       nil,
		IgnoreIfExists: false,
		OnBehalfOf:     nil,
	})
	require.NoError(t, err)

	err = agent.EnsureQueryIndexCreated(ctx, &gocbcorex.EnsureQueryIndexCreatedOptions{
		BucketName:     testutilsint.TestOpts.BucketName,
		ScopeName:      "_default",
		CollectionName: "_default",
		IndexName:      idxName,
		OnBehalfOf:     nil,
	})
	require.NoError(t, err)

	err = agent.DropIndex(ctx, &cbqueryx.DropIndexOptions{
		BucketName:        testutilsint.TestOpts.BucketName,
		ScopeName:         "_default",
		CollectionName:    "_default",
		IndexName:         idxName,
		IgnoreIfNotExists: false,
		OnBehalfOf:        nil,
	})
	require.NoError(t, err)

	err = agent.EnsureQueryIndexDropped(ctx, &gocbcorex.EnsureQueryIndexDroppedOptions{
		BucketName:     testutilsint.TestOpts.BucketName,
		ScopeName:      "_default",
		CollectionName: "_default",
		IndexName:      idxName,
		OnBehalfOf:     nil,
	})
	require.NoError(t, err)
}

func TestQueryMgmtPrimaryIndex(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	bucketName := testutilsint.TestOpts.BucketName
	idxName := uuid.NewString()[:6]

	t.Run("Create", func(t *testing.T) {
		err := agent.CreatePrimaryIndex(ctx, &cbqueryx.CreatePrimaryIndexOptions{
			BucketName:     bucketName,
			ScopeName:      "",
			CollectionName: "",
			IndexName:      "",
			NumReplicas:    nil,
			Deferred:       nil,
			IgnoreIfExists: true,
			OnBehalfOf:     nil,
		})
		require.NoError(t, err)
	})

	t.Run("CreateNamed", func(t *testing.T) {
		err := agent.CreatePrimaryIndex(ctx, &cbqueryx.CreatePrimaryIndexOptions{
			BucketName:     bucketName,
			ScopeName:      "",
			CollectionName: "",
			IndexName:      idxName,
			NumReplicas:    nil,
			Deferred:       nil,
			IgnoreIfExists: true,
			OnBehalfOf:     nil,
		})
		require.NoError(t, err)
	})

	t.Run("CreateExists", func(t *testing.T) {
		err := agent.CreatePrimaryIndex(ctx, &cbqueryx.CreatePrimaryIndexOptions{
			BucketName:     bucketName,
			ScopeName:      "",
			CollectionName: "",
			IndexName:      idxName,
			NumReplicas:    nil,
			Deferred:       nil,
			IgnoreIfExists: false,
			OnBehalfOf:     nil,
		})
		require.ErrorIs(t, err, cbqueryx.ErrIndexExists)
	})

	t.Run("CreateIgnoreExists", func(t *testing.T) {
		err := agent.CreatePrimaryIndex(ctx, &cbqueryx.CreatePrimaryIndexOptions{
			BucketName:     bucketName,
			ScopeName:      "",
			CollectionName: "",
			IndexName:      idxName,
			NumReplicas:    nil,
			Deferred:       nil,
			IgnoreIfExists: true,
			OnBehalfOf:     nil,
		})
		require.NoError(t, err)
	})

	t.Run("Drop", func(t *testing.T) {
		err := agent.DropPrimaryIndex(ctx, &cbqueryx.DropPrimaryIndexOptions{
			BucketName:        bucketName,
			ScopeName:         "",
			CollectionName:    "",
			IndexName:         "",
			IgnoreIfNotExists: false,
			OnBehalfOf:        nil,
		})
		require.NoError(t, err)
	})

	t.Run("DropNamed", func(t *testing.T) {
		err := agent.DropPrimaryIndex(ctx, &cbqueryx.DropPrimaryIndexOptions{
			BucketName:        bucketName,
			ScopeName:         "",
			CollectionName:    "",
			IndexName:         idxName,
			IgnoreIfNotExists: false,
			OnBehalfOf:        nil,
		})
		require.NoError(t, err)
	})

	t.Run("DropDoesntExist", func(t *testing.T) {
		err := agent.DropPrimaryIndex(ctx, &cbqueryx.DropPrimaryIndexOptions{
			BucketName:        bucketName,
			ScopeName:         "",
			CollectionName:    "",
			IndexName:         uuid.NewString()[:6],
			IgnoreIfNotExists: false,
			OnBehalfOf:        nil,
		})
		require.ErrorIs(t, err, cbqueryx.ErrIndexNotFound)
	})

	t.Run("DropIgnoreDoesntExist", func(t *testing.T) {
		err := agent.DropPrimaryIndex(ctx, &cbqueryx.DropPrimaryIndexOptions{
			BucketName:        bucketName,
			ScopeName:         "",
			CollectionName:    "",
			IndexName:         uuid.NewString()[:6],
			IgnoreIfNotExists: true,
			OnBehalfOf:        nil,
		})
		require.NoError(t, err)
	})
}

func TestQueryMgmtIndex(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	bucketName := testutilsint.TestOpts.BucketName
	idxName := uuid.NewString()[:6]

	t.Run("Create", func(t *testing.T) {
		err := agent.CreateIndex(ctx, &cbqueryx.CreateIndexOptions{
			BucketName:     bucketName,
			ScopeName:      "",
			CollectionName: "",
			IndexName:      idxName,
			Fields:         []string{"test"},
			NumReplicas:    nil,
			Deferred:       nil,
			IgnoreIfExists: true,
			OnBehalfOf:     nil,
		})
		require.NoError(t, err)
	})

	t.Run("CreateExists", func(t *testing.T) {
		err := agent.CreateIndex(ctx, &cbqueryx.CreateIndexOptions{
			BucketName:     bucketName,
			ScopeName:      "",
			CollectionName: "",
			IndexName:      idxName,
			Fields:         []string{"test"},
			NumReplicas:    nil,
			Deferred:       nil,
			IgnoreIfExists: false,
			OnBehalfOf:     nil,
		})
		require.ErrorIs(t, err, cbqueryx.ErrIndexExists)
	})

	t.Run("CreateIgnoreExists", func(t *testing.T) {
		err := agent.CreateIndex(ctx, &cbqueryx.CreateIndexOptions{
			BucketName:     bucketName,
			ScopeName:      "",
			CollectionName: "",
			IndexName:      idxName,
			Fields:         []string{"test"},
			NumReplicas:    nil,
			Deferred:       nil,
			IgnoreIfExists: true,
			OnBehalfOf:     nil,
		})
		require.NoError(t, err)
	})

	t.Run("Drop", func(t *testing.T) {
		err := agent.DropIndex(ctx, &cbqueryx.DropIndexOptions{
			BucketName:        bucketName,
			ScopeName:         "",
			CollectionName:    "",
			IndexName:         idxName,
			IgnoreIfNotExists: false,
			OnBehalfOf:        nil,
		})
		require.NoError(t, err)
	})

	t.Run("DropDoesntExist", func(t *testing.T) {
		err := agent.DropIndex(ctx, &cbqueryx.DropIndexOptions{
			BucketName:        bucketName,
			ScopeName:         "",
			CollectionName:    "",
			IndexName:         uuid.NewString()[:6],
			IgnoreIfNotExists: false,
			OnBehalfOf:        nil,
		})
		require.ErrorIs(t, err, cbqueryx.ErrIndexNotFound)
	})

	t.Run("DropIgnoreDoesntExist", func(t *testing.T) {
		err := agent.DropIndex(ctx, &cbqueryx.DropIndexOptions{
			BucketName:        bucketName,
			ScopeName:         "",
			CollectionName:    "",
			IndexName:         uuid.NewString()[:6],
			IgnoreIfNotExists: true,
			OnBehalfOf:        nil,
		})
		require.NoError(t, err)
	})
}

func TestQueryMgmtDeferredIndex(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	bucketName := testutilsint.TestOpts.BucketName
	idxName := uuid.NewString()[:6]

	trueBool := true
	err := agent.CreateIndex(ctx, &cbqueryx.CreateIndexOptions{
		BucketName:     bucketName,
		ScopeName:      "",
		CollectionName: "",
		IndexName:      idxName,
		Fields:         []string{"test"},
		NumReplicas:    nil,
		Deferred:       &trueBool,
		IgnoreIfExists: false,
		OnBehalfOf:     nil,
	})
	require.NoError(t, err)

	indexes, err := agent.BuildDeferredIndexes(ctx, &cbqueryx.BuildDeferredIndexesOptions{
		BucketName:     bucketName,
		ScopeName:      "",
		CollectionName: "",
		OnBehalfOf:     nil,
	})
	require.NoError(t, err)

	for _, idx := range indexes {
		if idx.IndexName == idxName && idx.BucketName == bucketName && idx.ScopeName == "_default" &&
			idx.CollectionName == "_default" {
			return
		}
	}

	require.Failf(t, "index was not found in list of deferred indexes", "indexes: %v", indexes)

	require.Eventually(t, func() bool {
		indexes, err := agent.GetAllIndexes(ctx, &cbqueryx.GetAllIndexesOptions{
			BucketName:     bucketName,
			ScopeName:      "",
			CollectionName: "",
			OnBehalfOf:     nil,
		})
		require.NoError(t, err)

		for _, idx := range indexes {
			if idx.Name == idxName && idx.KeyspaceId == bucketName {
				return idx.State == cbqueryx.IndexStateOnline
			}
		}

		return false
	}, 30*time.Second, 500*time.Millisecond)
}

// TestQueryNodePinning tests that the same node is used for multiple queries when the endpoint is pinned.
// We do this by performing one query to get an endpoint ID, then do another 10 queries and ensure that the
// same endpoint keeps being used.
func TestQueryNodePinning(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	res, err := agent.Query(ctx, &gocbcorex.QueryOptions{
		QueryOptions: cbqueryx.QueryOptions{
			Statement: "SELECT 1=1",
		},
		Endpoint: "",
	})
	require.NoError(t, err)

	firstEndpoint := res.Endpoint()
	require.NotEmpty(t, firstEndpoint)
	require.Regexp(t, `^quep-(.*)`, firstEndpoint)

	for i := 0; i < 10; i++ {
		res, err := agent.Query(ctx, &gocbcorex.QueryOptions{
			QueryOptions: cbqueryx.QueryOptions{
				Statement: "SELECT 1=1",
			},
			Endpoint: firstEndpoint,
		})
		require.NoError(t, err)

		endpoint := res.Endpoint()
		require.Equal(t, firstEndpoint, endpoint)
	}
}
