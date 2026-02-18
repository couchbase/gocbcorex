package gocbcorex_test

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	benchKeyPrefix = "bench-key-"
)

// intBenchAgent creates an agent for integration benchmarks
func intBenchAgent(b *testing.B) *gocbcorex.Agent {
	if !testutilsint.TestOpts.LongTest {
		b.Skipf("skipping long benchmark")
	}

	logger := zap.NewNop()

	opts := gocbcorex.AgentOptions{
		Logger:     logger,
		BucketName: testutilsint.TestOpts.BucketName,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: testutilsint.TestOpts.HTTPAddrs,
			MemdAddrs: testutilsint.TestOpts.MemdAddrs,
		},
		CompressionConfig: gocbcorex.CompressionConfig{
			EnableCompression: true,
		},
	}

	agent, err := gocbcorex.CreateAgent(b.Context(), opts)
	require.NoError(b, err)

	b.Cleanup(func() {
		err := agent.Close()
		if err != nil {
			b.Errorf("failed to close agent: %v", err)
		}
	})

	return agent
}

// intBenchCreateDocument creates a document for benchmarks that read
func intBenchCreateDocument(ctx context.Context, b *testing.B, agent *gocbcorex.Agent, key string, value []byte) {
	_, err := agent.Upsert(ctx, &gocbcorex.UpsertOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Value:          value,
	})
	require.NoError(b, err, "failed to create test document")
}

// intBenchCreateCounter creates a counter for benchmarks
func intBenchCreateCounter(ctx context.Context, b *testing.B, agent *gocbcorex.Agent, key string, value string) {
	_, err := agent.Add(ctx, &gocbcorex.AddOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(value),
	})
	require.NoError(b, err, "failed to create counter")
}

// ---------------------------------------------------------------------------
// Integration Benchmarks
// ---------------------------------------------------------------------------

// BenchmarkIntCrudGet measures Get end-to-end.
func BenchmarkIntCrudGet(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()
	intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))

	opts := &gocbcorex.GetOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Get(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudGetDecompress measures Get with decompression.
func BenchmarkIntCrudGetDecompress(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()
	intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchDoc1KB))

	opts := &gocbcorex.GetOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Get(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudUpsert measures Upsert with small payload.
func BenchmarkIntCrudUpsert(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(benchKeyPrefix + uuid.NewString())
	}

	docBytes := []byte(gocbcorex.BenchSmallDoc)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Upsert(ctx, &gocbcorex.UpsertOptions{
			Key:            keys[i],
			ScopeName:      "",
			CollectionName: "",
			Value:          docBytes,
		})
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudUpsertCompress measures Upsert with compression.
func BenchmarkIntCrudUpsertCompress(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()

	opts := &gocbcorex.UpsertOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(gocbcorex.BenchDoc1KB),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Upsert(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudLookupIn measures LookupIn with multiple operations.
func BenchmarkIntCrudLookupIn(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + "lookupin-" + uuid.NewString()
	intBenchCreateDocument(ctx, b, agent, key, []byte(`{"a":"x","b":1,"c":"y","d":2,"e":"z","f":3,"g":"w","h":4}`))

	opts := &gocbcorex.LookupInOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Ops: []memdx.LookupInOp{
			{Op: memdx.LookupInOpTypeGet, Path: []byte("a")},
			{Op: memdx.LookupInOpTypeGet, Path: []byte("b")},
			{Op: memdx.LookupInOpTypeGet, Path: []byte("c")},
			{Op: memdx.LookupInOpTypeGet, Path: []byte("d")},
			{Op: memdx.LookupInOpTypeGet, Path: []byte("e")},
			{Op: memdx.LookupInOpTypeGet, Path: []byte("f")},
			{Op: memdx.LookupInOpTypeGet, Path: []byte("g")},
			{Op: memdx.LookupInOpTypeGet, Path: []byte("h")},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.LookupIn(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudMutateIn measures MutateIn with multiple operations.
func BenchmarkIntCrudMutateIn(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + "mutatein-" + uuid.NewString()

	// Create initial document
	intBenchCreateDocument(ctx, b, agent, key, []byte(`{}`))

	opts := &gocbcorex.MutateInOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Ops: []memdx.MutateInOp{
			{Op: memdx.MutateInOpTypeDictSet, Path: []byte("a"), Value: []byte(`1`)},
			{Op: memdx.MutateInOpTypeDictSet, Path: []byte("b"), Value: []byte(`2`)},
			{Op: memdx.MutateInOpTypeDictSet, Path: []byte("c"), Value: []byte(`3`)},
			{Op: memdx.MutateInOpTypeDictSet, Path: []byte("d"), Value: []byte(`4`)},
			{Op: memdx.MutateInOpTypeDictSet, Path: []byte("e"), Value: []byte(`5`)},
			{Op: memdx.MutateInOpTypeDictSet, Path: []byte("f"), Value: []byte(`6`)},
			{Op: memdx.MutateInOpTypeDictSet, Path: []byte("g"), Value: []byte(`7`)},
			{Op: memdx.MutateInOpTypeDictSet, Path: []byte("h"), Value: []byte(`8`)},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.MutateIn(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudGetOrLookup measures GetOrLookup with projection.
func BenchmarkIntCrudGetOrLookup(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + "getorlookup-" + uuid.NewString()
	intBenchCreateDocument(ctx, b, agent, key, []byte(`{"foo":"bar"}`))

	opts := &gocbcorex.GetOrLookupOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Project:        []string{"foo"},
		WithFlags:      true,
		WithExpiry:     true,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.GetOrLookup(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudGetReplica measures GetReplica end-to-end.
func BenchmarkIntCrudGetReplica(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()
	intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))

	opts := &gocbcorex.GetReplicaOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		ReplicaIdx:     1,
	}

	// Wait for replication
	require.Eventually(b, func() bool {
		_, err := agent.GetReplica(ctx, opts)
		return err == nil
	}, 30*time.Second, 500*time.Millisecond, "replica did not become available in time")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.GetReplica(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudDelete measures Delete end-to-end.
func BenchmarkIntCrudDelete(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	// Pre-allocate keys and create documents before timing
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		key := benchKeyPrefix + uuid.NewString()
		keys[i] = []byte(key)
		intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Delete(ctx, &gocbcorex.DeleteOptions{
			Key:            keys[i],
			ScopeName:      "",
			CollectionName: "",
		})
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudGetAndTouch measures GetAndTouch end-to-end.
func BenchmarkIntCrudGetAndTouch(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()
	intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))

	opts := &gocbcorex.GetAndTouchOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Expiry:         3600,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.GetAndTouch(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudGetRandom measures GetRandom end-to-end.
func BenchmarkIntCrudGetRandom(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	// Create some documents for GetRandom to find
	for i := 0; i < 100; i++ {
		key := benchKeyPrefix + "random-" + uuid.NewString()
		intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))
	}

	opts := &gocbcorex.GetRandomOptions{
		ScopeName:      "",
		CollectionName: "",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.GetRandom(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudUnlock measures Unlock end-to-end.
func BenchmarkIntCrudUnlock(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	// Pre-allocate keys, create documents and lock them before timing
	keys := make([][]byte, b.N)
	casValues := make([]uint64, b.N)
	for i := 0; i < b.N; i++ {
		key := benchKeyPrefix + uuid.NewString()
		keys[i] = []byte(key)
		intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))

		lockRes, err := agent.GetAndLock(ctx, &gocbcorex.GetAndLockOptions{
			Key:            keys[i],
			ScopeName:      "",
			CollectionName: "",
			LockTime:       30,
		})
		require.NoError(b, err)
		casValues[i] = lockRes.Cas
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Unlock(ctx, &gocbcorex.UnlockOptions{
			Key:            keys[i],
			ScopeName:      "",
			CollectionName: "",
			Cas:            casValues[i],
		})
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudTouch measures Touch end-to-end.
func BenchmarkIntCrudTouch(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()
	intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))

	opts := &gocbcorex.TouchOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Expiry:         3600,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Touch(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudGetAndLock measures GetAndLock end-to-end.
func BenchmarkIntCrudGetAndLock(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	// Pre-allocate keys and create documents before timing
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		key := benchKeyPrefix + uuid.NewString()
		keys[i] = []byte(key)
		intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.GetAndLock(ctx, &gocbcorex.GetAndLockOptions{
			Key:            keys[i],
			ScopeName:      "",
			CollectionName: "",
			LockTime:       30,
		})
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudAdd measures Add end-to-end.
func BenchmarkIntCrudAdd(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	// Pre-allocate keys for the benchmark
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(benchKeyPrefix + uuid.NewString())
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Add(ctx, &gocbcorex.AddOptions{
			Key:            keys[i],
			ScopeName:      "",
			CollectionName: "",
			Value:          []byte(gocbcorex.BenchDoc1KB),
		})
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudReplace measures Replace end-to-end.
func BenchmarkIntCrudReplace(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()
	intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))

	opts := &gocbcorex.ReplaceOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(gocbcorex.BenchDoc1KB),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Replace(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudAppend measures Append end-to-end.
func BenchmarkIntCrudAppend(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()
	intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))

	opts := &gocbcorex.AppendOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte("append"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Append(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudPrepend measures Prepend end-to-end.
func BenchmarkIntCrudPrepend(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()
	intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))

	opts := &gocbcorex.PrependOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte("prepend"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Prepend(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudIncrement measures Increment end-to-end.
func BenchmarkIntCrudIncrement(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()
	intBenchCreateCounter(ctx, b, agent, key, "0")

	opts := &gocbcorex.IncrementOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Delta:          1,
		Initial:        0,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Increment(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudDecrement measures Decrement end-to-end.
func BenchmarkIntCrudDecrement(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()
	intBenchCreateCounter(ctx, b, agent, key, "100")

	opts := &gocbcorex.DecrementOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
		Delta:          1,
		Initial:        100,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.Decrement(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudGetMeta measures GetMeta end-to-end.
func BenchmarkIntCrudGetMeta(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()
	intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))

	opts := &gocbcorex.GetMetaOptions{
		Key:            []byte(key),
		ScopeName:      "",
		CollectionName: "",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.GetMeta(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudAddWithMeta measures AddWithMeta end-to-end.
func BenchmarkIntCrudAddWithMeta(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	// Pre-allocate keys for the benchmark
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(benchKeyPrefix + uuid.NewString())
	}

	docBytes := []byte(gocbcorex.BenchSmallDoc)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.AddWithMeta(ctx, &gocbcorex.AddWithMetaOptions{
			Key:            keys[i],
			ScopeName:      "",
			CollectionName: "",
			Value:          docBytes,
			StoreCas:       1,
		})
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudSetWithMeta measures SetWithMeta end-to-end.
func BenchmarkIntCrudSetWithMeta(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	key := benchKeyPrefix + uuid.NewString()

	docBytes := []byte(gocbcorex.BenchSmallDoc)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.SetWithMeta(ctx, &gocbcorex.SetWithMetaOptions{
			Key:            []byte(key),
			ScopeName:      "",
			CollectionName: "",
			Value:          docBytes,
			StoreCas:       1,
			Options:        memdx.MetaOpFlagSkipConflictResolution,
		})
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkIntCrudDeleteWithMeta measures DeleteWithMeta end-to-end.
func BenchmarkIntCrudDeleteWithMeta(b *testing.B) {
	ctx := context.Background()
	agent := intBenchAgent(b)

	// Pre-allocate keys and create documents before timing
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		key := benchKeyPrefix + uuid.NewString()
		keys[i] = []byte(key)
		intBenchCreateDocument(ctx, b, agent, key, []byte(gocbcorex.BenchSmallDoc))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := agent.DeleteWithMeta(ctx, &gocbcorex.DeleteWithMetaOptions{
			Key:            keys[i],
			ScopeName:      "",
			CollectionName: "",
			StoreCas:       1,
			Options:        memdx.MetaOpFlagSkipConflictResolution,
		})
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}
