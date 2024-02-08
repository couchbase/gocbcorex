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

	time.Sleep(time.Second * 2)

	ctx := context.Background()
	stream, err := agent.GetAllReplicas(ctx, &gocbcorex.GetAllReplicasOptions{
		Key:            []byte(docKey),
		BucketName:     "default",
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)

	result, err := stream.Next()
	masterCount := 0
	replicaCount := 0
	for result != nil {
		require.NoError(t, err)
		require.Equal(t, result.Value, []byte(`{"foo": "bar"}`))
		if !result.IsReplica {
			masterCount += 1
		} else {
			replicaCount += 1
		}
		result, err = stream.Next()
	}
	require.Equal(t, NUM_REPLICAS, masterCount+replicaCount)

	testutilsint.SkipIfNoDinoCluster(t)
	nodes := testutilsint.GetTestNodes(t)
	nonOrchestrators := nodes.Select(func(node *testutilsint.NodeTarget) bool {
		return !node.IsOrchestrator
	})

	dino := testutilsint.StartDinoTesting(t, true)
	for _, node := range nonOrchestrators {
		dino.BlockAllTraffic(node.Hostname)
	}

	time.Sleep(time.Second * 5)

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	stream, err = agent.GetAllReplicas(ctx, &gocbcorex.GetAllReplicasOptions{
		Key:            []byte(docKey),
		BucketName:     "default",
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)

	result, err = stream.Next()
	masterCount = 0
	for result != nil {
		require.NoError(t, err)
		require.Equal(t, result.Value, []byte(`{"foo": "bar"}`))
		masterCount += 1
		result, err = stream.Next()
	}
	require.Equal(t, 1, masterCount)
}
