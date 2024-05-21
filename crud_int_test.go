package gocbcorex_test

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const NUM_REPLICAS = 2

func TestGetAllReplicasDino(t *testing.T) {
	testutilsint.SkipIfNoDinoCluster(t)

	opts := CreateDefaultAgentOptions()
	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	defer func() {
		err := agent.Close()
		require.NoError(t, err)
	}()

	require.NoError(t, err)

	docKey := uuid.NewString()
	upsertRes, err := agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	require.Eventually(t, func() bool {
		count := replicaCount(t, agent, docKey)
		return count == NUM_REPLICAS+1
	}, time.Second*15, time.Second*1)

	nodes := testutilsint.GetTestNodes(t)
	nonOrchestrators := nodes.Select(func(node *testutilsint.NodeTarget) bool {
		return !node.IsOrchestrator
	})

	blocked := 0
	dino := testutilsint.StartDinoTesting(t, true)
	for _, node := range nonOrchestrators {
		dino.BlockAllTraffic(node.Hostname)
		blocked += 1

		require.Eventually(t, func() bool {
			count := replicaCount(t, agent, docKey)
			return count == NUM_REPLICAS+1-blocked
		}, time.Second*15, time.Second*1)
	}

	for _, node := range nonOrchestrators {
		dino.AllowTraffic(node.Hostname)
		blocked -= 1

		require.Eventually(t, func() bool {
			count := replicaCount(t, agent, docKey)
			return count == NUM_REPLICAS+1-blocked
		}, time.Second*30, time.Second*1)
	}
}

func replicaCount(t *testing.T, agent *gocbcorex.Agent, docKey string) int {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	stream, err := agent.GetAllReplicas(ctx, &gocbcorex.GetAllReplicasOptions{
		Key:            []byte(docKey),
		BucketName:     "default",
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)

	result, err := stream.Next()
	count := 0
	for result != nil {
		require.NoError(t, err)
		require.Equal(t, result.Value, []byte(`{"foo": "bar"}`))
		count += 1
		result, err = stream.Next()
	}
	return count
}
