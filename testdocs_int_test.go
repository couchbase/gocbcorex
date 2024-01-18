package gocbcorex_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/require"
)

type testDoc struct {
	TestName string `json:"testName"`
	I        int    `json:"i"`
}

type testDocs struct {
	t        *testing.T
	agent    *gocbcorex.Agent
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

			_, err = td.agent.Upsert(ctx, &gocbcorex.UpsertOptions{
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

			_, err := td.agent.Delete(ctx, &gocbcorex.DeleteOptions{
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

func makeTestDocs(ctx context.Context, t *testing.T, agent *gocbcorex.Agent, testName string, numDocs int) *testDocs {
	td := &testDocs{
		t:        t,
		agent:    agent,
		testName: testName,
		numDocs:  numDocs,
	}
	td.upsert(ctx)
	return td
}

type testBreweryGeoJson struct {
	Accuracy string  `json:"accuracy,omitempty"`
	Lat      float32 `json:"lat,omitempty"`
	Lon      float32 `json:"lon,omitempty"`
}

type testBreweryDocumentJson struct {
	City        string             `json:"city,omitempty"`
	Code        string             `json:"code,omitempty"`
	Country     string             `json:"country,omitempty"`
	Description string             `json:"description,omitempty"`
	Geo         testBreweryGeoJson `json:"geo,omitempty"`
	Name        string             `json:"name,omitempty"`
	Phone       string             `json:"phone,omitempty"`
	State       string             `json:"state,omitempty"`
	Type        string             `json:"type,omitempty"`
	Updated     string             `json:"updated,omitempty"`
	Website     string             `json:"website,omitempty"`

	Service string `json:"service,omitempty"`
}

type testBreweryDocs struct {
	t          *testing.T
	agent      *gocbcorex.Agent
	service    string
	docs       []testBreweryDocumentJson
	scope      string
	collection string
}

func makeBreweryTestDocs(ctx context.Context, t *testing.T, agent *gocbcorex.Agent, service, scopeName, collectionName string) *testBreweryDocs {
	raw := testutils.LoadTestData(t, "beer_sample_brewery_five.json")
	var dataset []testBreweryDocumentJson
	err := json.Unmarshal(raw, &dataset)
	require.NoError(t, err)

	td := &testBreweryDocs{
		t:          t,
		agent:      agent,
		service:    service,
		docs:       dataset,
		scope:      scopeName,
		collection: collectionName,
	}

	waitCh := make(chan error, len(dataset))

	for i, doc := range dataset {
		go func(i int, doc testBreweryDocumentJson) {
			testDocName := fmt.Sprintf("%s-%s", service, doc.Name)
			doc.Service = service

			bytes, err := json.Marshal(doc)
			require.NoError(t, err)

			_, err = td.agent.Upsert(ctx, &gocbcorex.UpsertOptions{
				Key:            []byte(testDocName),
				Value:          bytes,
				ScopeName:      scopeName,
				CollectionName: collectionName,
			})
			waitCh <- err
		}(i, doc)
	}

	for i := 1; i <= len(dataset); i++ {
		err := <-waitCh
		if err != nil {
			require.NoError(t, err)
		}
	}

	return td
}

func (td *testBreweryDocs) Remove(ctx context.Context) {
	waitCh := make(chan error, len(td.docs))

	for i, doc := range td.docs {
		go func(i int, doc testBreweryDocumentJson) {
			testDocName := fmt.Sprintf("%s-%s", td.service, doc.Name)

			_, err := td.agent.Delete(ctx, &gocbcorex.DeleteOptions{
				Key:            []byte(testDocName),
				ScopeName:      td.scope,
				CollectionName: td.collection,
			})
			waitCh <- err
		}(i, doc)
	}

	for i := 1; i <= len(td.docs); i++ {
		err := <-waitCh
		require.NoError(td.t, err)
	}
}
