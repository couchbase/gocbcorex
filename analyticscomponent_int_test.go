package gocbcorex_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/require"
)

type analyticsTestHelper struct {
	TestName string
	NumDocs  int
	TestDocs *testDocs
	Agent    *gocbcorex.Agent
	QueryFn  func(context.Context, *gocbcorex.AnalyticsQueryOptions) (gocbcorex.AnalyticsQueryResultStream, error)
	T        *testing.T
}

func hlpEnsureDataset(t *testing.T, agent *gocbcorex.Agent, bucketName string) {
	t.Helper()

	_, err := agent.AnalyticsQuery(context.Background(), &gocbcorex.AnalyticsQueryOptions{
		Statement: fmt.Sprintf("CREATE DATASET IF NOT EXISTS `%s` ON `%s`", bucketName, bucketName),
	})
	require.NoError(t, err)

	_, err = agent.AnalyticsQuery(context.Background(), &gocbcorex.AnalyticsQueryOptions{
		Statement: "CONNECT LINK Local",
	})
	require.NoError(t, err)
}

func (aqh *analyticsTestHelper) testSetupN1ql(t *testing.T) {
	aqh.TestDocs = makeTestDocs(context.Background(), t, aqh.Agent, aqh.TestName, aqh.NumDocs)

	hlpEnsureDataset(t, aqh.Agent, testutilsint.TestOpts.BucketName)
}

func (aqh *analyticsTestHelper) testCleanupN1ql(t *testing.T) {
	if aqh.TestDocs != nil {
		aqh.TestDocs.Remove(context.Background())
		aqh.TestDocs = nil
	}
}

func (aqh *analyticsTestHelper) testQueryBasic(t *testing.T) {
	deadline := time.Now().Add(30000 * time.Millisecond)
	runTestQuery := func() ([]testDoc, error) {
		iterDeadline := time.Now().Add(10000 * time.Millisecond)
		if iterDeadline.After(deadline) {
			iterDeadline = deadline
		}
		ctx, cancel := context.WithDeadline(context.Background(), iterDeadline)
		defer cancel()

		rows, err := aqh.QueryFn(ctx, &gocbcorex.AnalyticsQueryOptions{
			ClientContextId: "12345",
			Statement:       fmt.Sprintf("SELECT i,testName FROM %s WHERE testName=\"%s\"", testutilsint.TestOpts.BucketName, aqh.TestName),
		})
		if err != nil {
			aqh.T.Logf("Received error from analytics: %v", err)
			return nil, err
		}

		var docs []testDoc
		for {
			row, err := rows.ReadRow()
			if err != nil {
				return nil, err
			}

			if row == nil {
				aqh.T.Logf("Received now rows from analytics")
				break
			}

			var doc testDoc
			err = json.Unmarshal(row, &doc)
			if err != nil {
				aqh.T.Logf("Failed to unmarshal into testDoc: %v", err)
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
				if doc.I < 1 || doc.I > aqh.NumDocs {
					lastError = fmt.Sprintf("analytics test read invalid row i=%d", doc.I)
					testFailed = true
				}
			}

			numDocs := len(docs)
			if numDocs != aqh.NumDocs {
				aqh.T.Logf("Received incorrect number of rows. Expected: %d, received: %d", aqh.NumDocs, numDocs)
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

		if sleepDeadline.Equal(deadline) {
			t.Errorf("timed out waiting for indexing: %s", lastError)
			break
		}
	}
}

func TestAnalyticsBasic(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureAnalytics)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	helper := &analyticsTestHelper{
		TestName: "testAnalytics",
		NumDocs:  5,
		Agent:    agent,
		QueryFn:  agent.AnalyticsQuery,
		T:        t,
	}

	t.Run("setup", helper.testSetupN1ql)

	t.Run("Basic", helper.testQueryBasic)

	t.Run("cleanup", helper.testCleanupN1ql)
}
