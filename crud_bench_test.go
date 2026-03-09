package gocbcorex

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/golang/snappy"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// Minimal stubs for benchmarking (no locks, no tracking, no network)
// ---------------------------------------------------------------------------

type benchCollectionResolver struct {
	cid         uint32
	manifestRev uint64
}

func (r benchCollectionResolver) ResolveCollectionID(ctx context.Context, scopeName, collectionName string) (uint32, uint64, error) {
	return r.cid, r.manifestRev, nil
}

func (r benchCollectionResolver) InvalidateCollectionID(ctx context.Context, scopeName, collectionName, endpoint string, manifestRev uint64) {
}

type benchVbucketRouter struct {
	endpoint    string
	vbID        uint16
	numReplicas int
}

func (r benchVbucketRouter) UpdateRoutingInfo(*VbucketRoutingInfo) {}

func (r benchVbucketRouter) DispatchByKey(key []byte, vbServerIdx uint32) (string, uint16, error) {
	return r.endpoint, r.vbID, nil
}

func (r benchVbucketRouter) DispatchToVbucket(vbID uint16, vbServerIdx uint32) (string, error) {
	return r.endpoint, nil
}

func (r benchVbucketRouter) NumReplicas() (int, error) {
	return r.numReplicas, nil
}

type benchEndpointClientProvider struct {
	client KvClient
}

func (p benchEndpointClientProvider) GetEndpointClient(ctx context.Context, endpoint string) (KvClient, error) {
	return p.client, nil
}

// ---------------------------------------------------------------------------
// Mock MemdClient that immediately returns pre-built response packets
// This exercises the actual memdx.OpsCrud packet encoding/decoding code
// ---------------------------------------------------------------------------

type benchPendingOp struct{}

func (p *benchPendingOp) Cancel(err error) bool { return false }

type benchMemdClientTelem struct{}

func (t benchMemdClientTelem) BeginOp(ctx context.Context, bucketName string, opName string) (context.Context, MemdClientTelemOp) {
	return ctx, benchMemdClientTelemOp{}
}

type benchMemdClientTelemOp struct{}

func (t benchMemdClientTelemOp) IsRecording() bool                    { return false }
func (t benchMemdClientTelemOp) MarkSent()                            {}
func (t benchMemdClientTelemOp) MarkReceived()                        {}
func (t benchMemdClientTelemOp) RecordServerDuration(d time.Duration) {}
func (t benchMemdClientTelemOp) End(ctx context.Context, err error)   {}

// benchMemdClient implements MemdClient and immediately invokes the dispatch callback
// with a pre-built response packet, exercising the actual memdx.OpsCrud code paths.
type benchMemdClient struct {
	// Response packet builder - called to build the response based on OpCode
	buildResponse func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet
}

func (c *benchMemdClient) SelectedBucket() string { return "bench-bucket" }
func (c *benchMemdClient) Telemetry() MemdClientTelem {
	return benchMemdClientTelem{}
}

func (c *benchMemdClient) Dispatch(req *memdx.Packet, cb memdx.DispatchCallback) (memdx.PendingOp, error) {
	// Immediately invoke callback with pre-built response
	resp := c.buildResponse(req.OpCode, req)
	cb(resp, nil)
	return &benchPendingOp{}, nil
}

func (c *benchMemdClient) WritePacket(pak *memdx.Packet) error { panic("unused") }
func (c *benchMemdClient) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
}
func (c *benchMemdClient) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 11210}
}
func (c *benchMemdClient) Close() error { return nil }

// newBenchKvClient creates a KvClient using NewKvClient with our mock MemdClient.
// This exercises the actual kvClient code paths with packet encoding/decoding.
func newBenchKvClient(buildResponse func(memdx.OpCode, *memdx.Packet) *memdx.Packet) KvClient {
	return newBenchKvClientWithFeatures(benchFeatures, buildResponse)
}

// newBenchKvClientWithFeatures creates a KvClient with custom features enabled.
func newBenchKvClientWithFeatures(features []memdx.HelloFeature, buildResponse func(memdx.OpCode, *memdx.Packet) *memdx.Packet) KvClient {
	kv, err := NewKvClient(context.Background(), &KvClientOptions{
		Logger:         zap.NewNop(),
		Address:        "bench-host:11210",
		SelectedBucket: "bench-bucket",
		BootstrapOpts: KvClientBootstrapOptions{
			ClientName:             "bench-client",
			DisableDefaultFeatures: false,
			DisableErrorMap:        true,
			DisableOutOfOrderExec:  true,
		},
		DialMemdxClient: benchDialMemdxClient(features, buildResponse),
	})
	if err != nil {
		panic(err)
	}
	return kv
}

// benchDialMemdxClient is a custom dial function that returns our benchMemdClient
func benchDialMemdxClient(features []memdx.HelloFeature, buildResponse func(memdx.OpCode, *memdx.Packet) *memdx.Packet) DialMemdxClientFunc {
	return func(ctx context.Context, address string, dialOpts *memdx.DialConnOptions, clientOpts *memdx.ClientOptions) (MemdxClient, error) {
		return &benchMemdClient{buildResponse: benchBuildResponse(features, buildResponse)}, nil
	}
}

// makeHelloResponse builds a Hello response with enabled features
func makeHelloResponse(features []memdx.HelloFeature) *memdx.Packet {
	featureBytes := make([]byte, len(features)*2)
	for i, f := range features {
		binary.BigEndian.PutUint16(featureBytes[i*2:], uint16(f))
	}
	return &memdx.Packet{
		Status: memdx.StatusSuccess,
		Value:  featureBytes,
	}
}

// benchFeatures are the default features we enable for benchmarking
var benchFeatures = []memdx.HelloFeature{
	memdx.HelloFeatureDatatype,
	memdx.HelloFeatureSeqNo,
	memdx.HelloFeatureSnappy,
	memdx.HelloFeatureCollections,
	memdx.HelloFeatureAltRequests,
	memdx.HelloFeatureSyncReplication,
	memdx.HelloFeaturePreserveExpiry,
}

// benchBuildResponse wraps a user-provided buildResponse to also handle bootstrap opcodes
func benchBuildResponse(features []memdx.HelloFeature, crudFn func(memdx.OpCode, *memdx.Packet) *memdx.Packet) func(memdx.OpCode, *memdx.Packet) *memdx.Packet {
	return func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		switch opCode {
		case memdx.OpCodeHello:
			return makeHelloResponse(features)
		case memdx.OpCodeSelectBucket:
			return benchSuccessPacket
		default:
			return crudFn(opCode, req)
		}
	}
}

// ---------------------------------------------------------------------------
// Pre-built response packets for each operation type
// ---------------------------------------------------------------------------

// benchSuccessPacket is a pre-built success response used as fallback
var benchSuccessPacket = &memdx.Packet{Status: memdx.StatusSuccess}

// makeMutationExtras builds the 16-byte extras for mutation responses (vbUuid + seqNo)
func makeMutationExtras(vbUuid, seqNo uint64) []byte {
	extras := make([]byte, 16)
	binary.BigEndian.PutUint64(extras[0:], vbUuid)
	binary.BigEndian.PutUint64(extras[8:], seqNo)
	return extras
}

func getResponse(value []byte, flags uint32, datatype uint8, cas uint64) *memdx.Packet {
	extras := make([]byte, 4)
	binary.BigEndian.PutUint32(extras, flags)
	return &memdx.Packet{
		Status:   memdx.StatusSuccess,
		Extras:   extras,
		Value:    value,
		Datatype: datatype,
		Cas:      cas,
	}
}

func setResponse(cas uint64, vbUuid uint64, seqNo uint64) *memdx.Packet {
	return &memdx.Packet{
		Status: memdx.StatusSuccess,
		Extras: makeMutationExtras(vbUuid, seqNo),
		Cas:    cas,
	}
}

func deleteResponse(cas uint64, vbUuid uint64, seqNo uint64) *memdx.Packet {
	return &memdx.Packet{
		Status: memdx.StatusSuccess,
		Extras: makeMutationExtras(vbUuid, seqNo),
		Cas:    cas,
	}
}

func counterResponse(cas uint64, value uint64, vbUuid uint64, seqNo uint64) *memdx.Packet {
	valueBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(valueBytes, value)
	return &memdx.Packet{
		Status: memdx.StatusSuccess,
		Extras: makeMutationExtras(vbUuid, seqNo),
		Value:  valueBytes,
		Cas:    cas,
	}
}

func getRandomResponse(key []byte, value []byte, flags uint32, datatype uint8, cas uint64) *memdx.Packet {
	extras := make([]byte, 4)
	binary.BigEndian.PutUint32(extras, flags)
	return &memdx.Packet{
		Status:   memdx.StatusSuccess,
		Key:      key,
		Extras:   extras,
		Value:    value,
		Datatype: datatype,
		Cas:      cas,
	}
}

func getMetaResponse(value []byte, flags uint32, cas uint64, expiry uint32, seqNo uint64) *memdx.Packet {
	extras := make([]byte, 20)
	binary.BigEndian.PutUint32(extras[0:], 0) // isDeleted
	binary.BigEndian.PutUint32(extras[4:], flags)
	binary.BigEndian.PutUint32(extras[8:], expiry)
	binary.BigEndian.PutUint64(extras[12:], seqNo)
	return &memdx.Packet{
		Status: memdx.StatusSuccess,
		Extras: extras,
		Value:  value,
		Cas:    cas,
	}
}

func lookupInResponse(ops []memdx.SubDocResult, cas uint64) *memdx.Packet {
	// Build the value as a series of subdoc results
	// Format: each result has 2 bytes status, 4 bytes value length, then value
	value := make([]byte, 0, 256)
	for _, op := range ops {
		var status memdx.Status
		if op.Err != nil {
			status = subDocErrorToStatus(op.Err)
		}
		value = binary.BigEndian.AppendUint16(value, uint16(status))
		value = binary.BigEndian.AppendUint32(value, uint32(len(op.Value)))
		value = append(value, op.Value...)
	}

	return &memdx.Packet{
		Status: memdx.StatusSuccess,
		Value:  value,
		Cas:    cas,
	}
}

func subDocErrorToStatus(err error) memdx.Status {
	switch err {
	case memdx.ErrSubDocPathNotFound:
		return memdx.StatusSubDocPathNotFound
	case memdx.ErrSubDocDocTooDeep:
		return memdx.StatusSubDocDocTooDeep
	default:
		return memdx.StatusSubDocPathNotFound
	}
}

func mutateInResponse(ops []memdx.SubDocResult, cas uint64, vbUuid uint64, seqNo uint64) *memdx.Packet {
	// Build the value as a series of subdoc results
	// Format: 1 byte opIndex, 2 bytes status, then if success: 4 bytes value length, then value
	value := make([]byte, 0, 256)
	for i, op := range ops {
		value = append(value, byte(i)) // opIndex
		var status memdx.Status
		if op.Err != nil {
			status = subDocErrorToStatus(op.Err)
		}
		value = binary.BigEndian.AppendUint16(value, uint16(status))
		value = binary.BigEndian.AppendUint32(value, uint32(len(op.Value)))
		value = append(value, op.Value...)
	}

	return &memdx.Packet{
		Status: memdx.StatusSuccess,
		Extras: makeMutationExtras(vbUuid, seqNo),
		Value:  value,
		Cas:    cas,
	}
}

// ---------------------------------------------------------------------------
// Helper constructor for CrudComponent
// ---------------------------------------------------------------------------

func newBenchCrudComponent(kv KvClient, snappyEnabled bool) *CrudComponent {
	return &CrudComponent{
		logger:          zap.NewNop(),
		disableMetrics:  true,
		retries:         NewRetryManagerFastFail(),
		collections:     benchCollectionResolver{cid: 0x100, manifestRev: 1},
		vbs:             benchVbucketRouter{endpoint: "bench-ep", vbID: 42, numReplicas: 0},
		eclientProvider: benchEndpointClientProvider{client: kv},
		nmvHandler:      nil,
		vbc:             nil,
		compression: &CompressionManagerDefault{
			compressionMinSize:   32,
			compressionMinRatio:  0.83,
			disableCompression:   !snappyEnabled,
			disableDecompression: false,
		},
	}
}

// ---------------------------------------------------------------------------
// Pre-computed fixtures
// ---------------------------------------------------------------------------

var (
	benchKey = []byte("benchmark-key")

	// 1KB compressible JSON document
	BenchDoc1KB = func() []byte {
		b := make([]byte, 0, 1024)
		b = append(b, `{"name":"benchmark","data":"`...)
		// Fill with repeated 'x' to make it compressible
		for len(b) < 1000 {
			b = append(b, "xxxxxxxxxx"...)
		}
		b = append(b, `"}`...)
		return b
	}()

	// Snappy-compressed version
	benchDoc1KBSnappy = snappy.Encode(nil, BenchDoc1KB)

	// Small doc (<32 bytes, won't compress)
	BenchSmallDoc = []byte(`{"v":1}`)

	// Constant values for responses
	benchCas    uint64 = 1
	benchVbUuid uint64 = 12345
	benchSeqNo  uint64 = 100
)

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

// BenchmarkOrchestrateSimpleCrud measures the orchestration overhead
// for a simple CRUD operation (collection resolution + routing + endpoint client).
func BenchmarkOrchestrateSimpleCrud(b *testing.B) {
	ctx := context.Background()
	rs := NewRetryManagerFastFail()
	cr := benchCollectionResolver{cid: 0x100, manifestRev: 1}
	vb := benchVbucketRouter{endpoint: "bench-ep", vbID: 42}
	kv := newBenchKvClient(func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		return benchSuccessPacket
	})
	ecp := benchEndpointClientProvider{client: kv}

	fn := func(collectionID uint32, endpoint string, vbID uint16, client KvClient) (uint64, error) {
		return 1, nil
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := OrchestrateSimpleCrud(ctx, rs, cr, vb, nil, ecp, "", "", 0, benchKey, fn)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudGet measures Get when AcceptSnappy=true (skips decompression).
func BenchmarkCrudGet(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClient(func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeGet {
			return getResponse(benchDoc1KBSnappy, 0, uint8(memdx.DatatypeFlagCompressed), benchCas)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, true)

	opts := &GetOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		AcceptSnappy: true,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Get(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudGetDecompress measures Get when decompression is required.
func BenchmarkCrudGetDecompress(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClient(func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeGet {
			return getResponse(benchDoc1KBSnappy, 0, uint8(memdx.DatatypeFlagCompressed), benchCas)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, true)

	opts := &GetOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		AcceptSnappy: false,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Get(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudUpsertCompress measures Upsert with compression enabled and compressible data.
func BenchmarkCrudUpsertCompress(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClient(func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeSet {
			return setResponse(benchCas, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, true)

	opts := &UpsertOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Value:        BenchDoc1KB,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Upsert(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudUpsertNoCompress measures Upsert with small payload (<32 bytes).
func BenchmarkCrudUpsertNoCompress(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClient(func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeSet {
			return setResponse(benchCas, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, true)

	opts := &UpsertOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Value:        BenchSmallDoc,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Upsert(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudLookupIn measures LookupIn with multiple operations.
func BenchmarkCrudLookupIn(b *testing.B) {
	ctx := context.Background()

	subdocResults := []memdx.SubDocResult{
		{Value: []byte(`"x"`)},
		{Value: []byte(`1`)},
		{Value: []byte(`"y"`)},
		{Value: []byte(`2`)},
		{Value: []byte(`"z"`)},
		{Value: []byte(`3`)},
		{Value: []byte(`"w"`)},
		{Value: []byte(`4`)},
	}

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeSubDocMultiLookup {
			return lookupInResponse(subdocResults, benchCas)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &LookupInOptions{
		Key:          benchKey,
		CollectionID: 0x100,
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
		_, err := cc.LookupIn(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudMutateIn measures MutateIn with multiple operations.
func BenchmarkCrudMutateIn(b *testing.B) {
	ctx := context.Background()

	subdocResults := []memdx.SubDocResult{{}, {}, {}, {}, {}, {}, {}, {}}

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
		memdx.HelloFeatureSyncReplication,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeSubDocMultiMutation {
			return mutateInResponse(subdocResults, benchCas, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &MutateInOptions{
		Key:          benchKey,
		CollectionID: 0x100,
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
		_, err := cc.MutateIn(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudGetOrLookup0Paths measures GetOrLookup when no projection is needed
// (delegates directly to Get).
func BenchmarkCrudGetOrLookup0Paths(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeGet {
			return getResponse(BenchSmallDoc, 0, 0, benchCas)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &GetOrLookupOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Project:      nil,
		WithExpiry:   false,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.GetOrLookup(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudGetOrLookup1Path measures projection with 1 path + flags + expiry.
func BenchmarkCrudGetOrLookup1Path(b *testing.B) {
	ctx := context.Background()

	subdocResults := []memdx.SubDocResult{
		{Value: []byte("123")}, // flags xattr
		{Value: []byte("456")}, // expiry xattr
		{Value: []byte(`"x"`)}, // foo
	}

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeSubDocMultiLookup {
			return lookupInResponse(subdocResults, benchCas)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &GetOrLookupOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Project:      []string{"foo"},
		WithFlags:    true,
		WithExpiry:   true,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.GetOrLookup(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudGetEx measures GetEx which always decompresses the response.
func BenchmarkCrudGetEx(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureSnappy,
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeGetEx {
			return getResponse(benchDoc1KBSnappy, 0, uint8(memdx.DatatypeFlagCompressed), benchCas)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, true)

	opts := &GetExOptions{
		Key:          benchKey,
		CollectionID: 0x100,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.GetEx(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudGetReplica measures GetReplica which uses a different routing path.
func BenchmarkCrudGetReplica(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeGetReplica {
			return getResponse(BenchSmallDoc, 0, 0, benchCas)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &GetReplicaOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		ReplicaIdx:   0,
		AcceptSnappy: false,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.GetReplica(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudDelete measures the Delete operation.
func BenchmarkCrudDelete(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
		memdx.HelloFeatureSyncReplication,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeDelete {
			return deleteResponse(benchCas, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &DeleteOptions{
		Key:          benchKey,
		CollectionID: 0x100,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Delete(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudGetAndTouch measures GetAndTouch which combines get with expiry update.
func BenchmarkCrudGetAndTouch(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeGAT {
			return getResponse(BenchSmallDoc, 0, 0, benchCas)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &GetAndTouchOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Expiry:       3600,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.GetAndTouch(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudGetRandom measures GetRandom which fetches a random document.
func BenchmarkCrudGetRandom(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeGetRandom {
			return getRandomResponse([]byte("randomkey"), BenchSmallDoc, 0, 0, benchCas)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &GetRandomOptions{
		CollectionID: 0x100,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.GetRandom(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudUnlock measures Unlock which releases a locked document.
func BenchmarkCrudUnlock(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
		memdx.HelloFeatureSyncReplication,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeUnlockKey {
			return &memdx.Packet{
				Status: memdx.StatusSuccess,
				Extras: makeMutationExtras(benchVbUuid, benchSeqNo),
			}
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &UnlockOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Cas:          benchCas,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Unlock(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudTouch measures Touch which updates expiry without fetching.
func BenchmarkCrudTouch(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeTouch {
			return &memdx.Packet{
				Status: memdx.StatusSuccess,
				Cas:    benchCas,
			}
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &TouchOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Expiry:       3600,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Touch(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudGetAndLock measures GetAndLock which locks a document while reading.
func BenchmarkCrudGetAndLock(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeGetLocked {
			return getResponse(BenchSmallDoc, 0, 0, benchCas)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &GetAndLockOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		LockTime:     30,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.GetAndLock(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudAdd measures Add which only succeeds if document doesn't exist.
func BenchmarkCrudAdd(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureSnappy,
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
		memdx.HelloFeatureSyncReplication,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeAdd {
			return setResponse(benchCas, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, true)

	opts := &AddOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Value:        BenchDoc1KB,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Add(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudReplace measures Replace which only succeeds if document exists.
func BenchmarkCrudReplace(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureSnappy,
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
		memdx.HelloFeatureSyncReplication,
		memdx.HelloFeaturePreserveExpiry,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeReplace {
			return setResponse(benchCas, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, true)

	opts := &ReplaceOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Value:        BenchDoc1KB,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Replace(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudAppend measures Append which appends data to existing document.
func BenchmarkCrudAppend(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureSnappy,
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
		memdx.HelloFeatureSyncReplication,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeAppend {
			return setResponse(benchCas, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, true)

	opts := &AppendOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Value:        BenchSmallDoc,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Append(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudPrepend measures Prepend which prepends data to existing document.
func BenchmarkCrudPrepend(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureSnappy,
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
		memdx.HelloFeatureSyncReplication,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodePrepend {
			return setResponse(benchCas, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, true)

	opts := &PrependOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Value:        BenchSmallDoc,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Prepend(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudIncrement measures Increment which increments a counter.
func BenchmarkCrudIncrement(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
		memdx.HelloFeatureSyncReplication,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeIncrement {
			return counterResponse(benchCas, 1, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &IncrementOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Delta:        1,
		Initial:      0,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Increment(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudDecrement measures Decrement which decrements a counter.
func BenchmarkCrudDecrement(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
		memdx.HelloFeatureSyncReplication,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeDecrement {
			return counterResponse(benchCas, 0, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &DecrementOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Delta:        1,
		Initial:      0,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.Decrement(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudGetMeta measures GetMeta which fetches document metadata.
func BenchmarkCrudGetMeta(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeGetMeta {
			return getMetaResponse(BenchSmallDoc, 0, benchCas, 0, 1)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &GetMetaOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		VBUUID:       0, // skip consistency check
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.GetMeta(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudAddWithMeta measures AddWithMeta which adds with explicit metadata.
func BenchmarkCrudAddWithMeta(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeAddWithMeta {
			return setResponse(benchCas, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &AddWithMetaOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Value:        BenchSmallDoc,
		VBUUID:       0, // skip consistency check
		StoreCas:     1,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.AddWithMeta(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudSetWithMeta measures SetWithMeta which sets with explicit metadata.
func BenchmarkCrudSetWithMeta(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeSetWithMeta {
			return setResponse(benchCas, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &SetWithMetaOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		Value:        BenchSmallDoc,
		VBUUID:       0, // skip consistency check
		StoreCas:     1,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.SetWithMeta(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudDeleteWithMeta measures DeleteWithMeta which deletes with explicit metadata.
func BenchmarkCrudDeleteWithMeta(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeDelWithMeta {
			return deleteResponse(benchCas, benchVbUuid, benchSeqNo)
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &DeleteWithMetaOptions{
		Key:          benchKey,
		CollectionID: 0x100,
		VBUUID:       0, // skip consistency check
		StoreCas:     1,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.DeleteWithMeta(ctx, opts)
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}

// BenchmarkCrudStatsByVbucket measures StatsByVbucket which queries stats by vbucket.
func BenchmarkCrudStatsByVbucket(b *testing.B) {
	ctx := context.Background()

	kv := newBenchKvClientWithFeatures([]memdx.HelloFeature{
		memdx.HelloFeatureCollections,
		memdx.HelloFeatureAltRequests,
	}, func(opCode memdx.OpCode, req *memdx.Packet) *memdx.Packet {
		if opCode == memdx.OpCodeStat {
			return &memdx.Packet{
				Status: memdx.StatusSuccess,
			}
		}
		return benchSuccessPacket
	})
	cc := newBenchCrudComponent(kv, false)

	opts := &StatsByVbucketOptions{
		VbucketID: 42,
		GroupName: "items",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := cc.StatsByVbucket(ctx, opts, func(StatsDataResult) {})
		if err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
	}
}
