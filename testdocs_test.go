package core

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

type testDoc struct {
	TestName string `json:"testName"`
	I        int    `json:"i"`
}

type testDocs struct {
	t        *testing.T
	agent    *Agent
	testName string
	numDocs  int
}

func (td *testDocs) upsert(ctx context.Context) {
	waitCh := make(chan error, td.numDocs)

	for i := 1; i <= td.numDocs; i++ {
		go func(i int) {
			testDocName := fmt.Sprintf("%s-%d", td.testName, i)

			bytes, err := json.Marshal(testDoc{
				TestName: td.testName,
				I:        i,
			})
			if err != nil {
				td.t.Errorf("failed to marshal test doc: %v", err)
				return
			}

			_, err = td.agent.Upsert(ctx, &UpsertOptions{
				Key:   []byte(testDocName),
				Value: bytes,
			})
			waitCh <- err
		}(i)
	}

	for i := 1; i <= td.numDocs; i++ {
		err := <-waitCh
		if err != nil {
			td.t.Errorf("failed to remove test doc: %v", err)
			return
		}
	}
}

func (td *testDocs) Remove(ctx context.Context) {
	waitCh := make(chan error, td.numDocs)

	for i := 1; i <= td.numDocs; i++ {
		go func(i int) {
			testDocName := fmt.Sprintf("%s-%d", td.testName, i)

			_, err := td.agent.Delete(ctx, &DeleteOptions{
				Key: []byte(testDocName),
			})
			waitCh <- err
		}(i)
	}

	for i := 1; i <= td.numDocs; i++ {
		err := <-waitCh
		if err != nil {
			td.t.Errorf("failed to remove test doc: %v", err)
			return
		}
	}
}

func makeTestDocs(ctx context.Context, t *testing.T, agent *Agent, testName string, numDocs int) *testDocs {
	td := &testDocs{
		t:        t,
		agent:    agent,
		testName: testName,
		numDocs:  numDocs,
	}
	td.upsert(ctx)
	return td
}
