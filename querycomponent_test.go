package gocbcorex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/require"
)

type n1qlTestHelper struct {
	TestName      string
	NumDocs       int
	QueryTestDocs *testDocs
	Agent         *Agent
	QueryFn       func(context.Context, *QueryOptions) (QueryResultStream, error)
	T             *testing.T
}

func hlpRunQuery(t *testing.T, agent *Agent, opts *QueryOptions) ([][]byte, error) {
	t.Helper()

	rows, err := agent.Query(context.Background(), opts)
	if err != nil {
		return nil, err
	}

	var rowBytes [][]byte
	for {
		row, err := rows.ReadRow()
		if err != nil {
			return nil, err
		}

		if row == nil {
			break
		}

		rowBytes = append(rowBytes, row)
	}

	return rowBytes, nil
}

func hlpEnsurePrimaryIndex(t *testing.T, agent *Agent, bucketName string) {
	t.Helper()

	_, err := hlpRunQuery(t, agent, &QueryOptions{
		Statement: "CREATE PRIMARY INDEX ON " + bucketName,
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		require.NoError(t, err)
	}
}

func (nqh *n1qlTestHelper) testSetupN1ql(t *testing.T) {
	nqh.QueryTestDocs = makeTestDocs(context.Background(), t, nqh.Agent, nqh.TestName, nqh.NumDocs)

	hlpEnsurePrimaryIndex(t, nqh.Agent, testutils.TestOpts.BucketName)
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

		rows, err := nqh.QueryFn(ctx, &QueryOptions{
			ClientContextId: "12345",
			Statement:       fmt.Sprintf("SELECT i,testName FROM %s WHERE testName=\"%s\"", testutils.TestOpts.BucketName, nqh.TestName),
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
	testutils.SkipIfShortTest(t)

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
	testutils.SkipIfShortTest(t)

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
	testutils.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	idxName := uuid.NewString()[:6]
	res, err := agent.Query(ctx, &QueryOptions{
		Statement: fmt.Sprintf(
			"CREATE INDEX `%s` On `%s`._default._default(test)",
			idxName,
			testutils.TestOpts.BucketName,
		),
	})
	if errors.Is(err, cbqueryx.ErrBuildAlreadyInProgress) {
		// the build is delayed, we need to wait
	} else {
		require.NoError(t, err)

		for res.HasMoreRows() {
		}
	}

	err = agent.EnsureQueryIndexCreated(ctx, &EnsureQueryIndexCreatedOptions{
		BucketName:     testutils.TestOpts.BucketName,
		ScopeName:      "_default",
		CollectionName: "_default",
		IndexName:      idxName,
		OnBehalfOf:     nil,
	})
	require.NoError(t, err)

	res, err = agent.Query(ctx, &QueryOptions{
		Statement: fmt.Sprintf(
			"DROP INDEX `%s` ON `%s`.`_default`.`_default`",
			idxName,
			testutils.TestOpts.BucketName,
		),
	})
	require.NoError(t, err)

	for res.HasMoreRows() {
	}

	err = agent.EnsureQueryIndexDropped(ctx, &EnsureQueryIndexDroppedOptions{
		BucketName:     testutils.TestOpts.BucketName,
		ScopeName:      "_default",
		CollectionName: "_default",
		IndexName:      idxName,
		OnBehalfOf:     nil,
	})
	require.NoError(t, err)
}
