package memdx

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/gocbcorex/testutils"
)

func TestOpBootstrapPlainAuth(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)

	opts := makeDefaultBootstrapOptions()

	res, err := syncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	require.NoError(t, err)

	assert.Equal(t, enc.Responses.Hello.Resp.EnabledFeatures, res.Hello.EnabledFeatures)
	assert.Equal(t, errmap, res.ErrorMap)
	assert.Equal(t, cfg, res.ClusterConfig)
}

func TestOpBootstrapHelloFails(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.Responses.Hello = unaryResult[*HelloResponse]{
		Err: errors.New("i failed"),
	}

	opts := makeDefaultBootstrapOptions()

	res, err := syncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	require.NoError(t, err)

	assert.Nil(t, res.Hello)
	assert.Equal(t, errmap, res.ErrorMap)
	assert.Equal(t, cfg, res.ClusterConfig)
}

func TestOpBootstrapGetErrMapFails(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.Responses.GetErrorMap = unaryResult[[]byte]{
		Err: errors.New("i failed"),
	}

	opts := makeDefaultBootstrapOptions()

	res, err := syncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	require.NoError(t, err)

	assert.Equal(t, enc.Responses.Hello.Resp.EnabledFeatures, res.Hello.EnabledFeatures)
	assert.Nil(t, res.ErrorMap)
	assert.Equal(t, cfg, res.ClusterConfig)
}

func TestOpBootstrapListMechFails(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.Responses.SASLListMech = unaryResult[*SASLListMechsResponse]{
		Err: errors.New("error"),
	}

	opts := makeDefaultBootstrapOptions()

	res, err := syncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	require.NoError(t, err)

	assert.Equal(t, enc.Responses.Hello.Resp.EnabledFeatures, res.Hello.EnabledFeatures)
	assert.Equal(t, errmap, res.ErrorMap)
	assert.Equal(t, cfg, res.ClusterConfig)
}

func TestOpBootstrapAuthFails(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.Responses.SASLAuth = unaryResult[*SASLAuthResponse]{
		Err: ErrAuthError,
	}

	opts := makeDefaultBootstrapOptions()

	_, err := syncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	assert.ErrorIs(t, err, ErrAuthError)
}

func TestOpBootstrapSelectBucketFails(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.Responses.SelectBucket = errors.New("imnobucket")

	opts := makeDefaultBootstrapOptions()

	_, err := syncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	assert.ErrorIs(t, err, enc.Responses.SelectBucket)
}

func TestOpBootstrapGetClusterConfigFails(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.Responses.GetClusterConfig = unaryResult[[]byte]{
		Err: errors.New("i failed"),
	}

	opts := makeDefaultBootstrapOptions()

	res, err := syncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	require.NoError(t, err)

	assert.Equal(t, enc.Responses.Hello.Resp.EnabledFeatures, res.Hello.EnabledFeatures)
	assert.Equal(t, errmap, res.ErrorMap)
	assert.Nil(t, res.ClusterConfig)
}

func TestOpBootstrapCancelled(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.HelloBlockCh = &bootstrapCoordinationPair{
		Reached:  make(chan struct{}, 1),
		Continue: make(chan struct{}, 1),
	}

	opts := makeDefaultBootstrapOptions()

	type test struct {
		Name            string
		TargetFieldName string
	}

	tests := []test{
		{
			Name:            "Hello",
			TargetFieldName: "HelloBlockCh",
		},
		{
			Name:            "ErrMap",
			TargetFieldName: "ErrMapBlockCh",
		},
		{
			Name:            "SASLListMechs",
			TargetFieldName: "SASLListBlockCh",
		},
		{
			Name:            "SASLAuth",
			TargetFieldName: "SASLAuthBlockCh",
		},
		{
			Name:            "SelectBucket",
			TargetFieldName: "SelectBucketBlockCh",
		},
		{
			Name:            "GetClusterConfig",
			TargetFieldName: "GetClusterConfigBlockCh",
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			pair := &bootstrapCoordinationPair{
				Reached:  make(chan struct{}, 1),
				Continue: make(chan struct{}, 1),
			}
			enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
			elem := reflect.ValueOf(enc).Elem()
			elem.FieldByName(test.TargetFieldName).Set(reflect.ValueOf(pair))

			wait := make(chan unaryResult[*BootstrapResult], 1)
			op, err := OpBootstrap{
				Encoder: enc,
			}.Bootstrap(testBootstrapDispatcher{}, opts, func(res *BootstrapResult, err error) {
				wait <- unaryResult[*BootstrapResult]{
					Resp: res,
					Err:  err,
				}
			})
			require.NoError(tt, err)

			expectedErr := errors.New("some error")
			<-pair.Reached
			op.Cancel(requestCancelledError{expectedErr})
			pair.Continue <- struct{}{}

			res := <-wait
			assert.ErrorIs(tt, res.Err, expectedErr)
		})
	}
}

func makeDefaultBootstrapOptions() *BootstrapOptions {
	opts := &BootstrapOptions{
		Hello: &HelloRequest{
			ClientName:        []byte("test-client"),
			RequestedFeatures: []HelloFeature{1, 2, 3, 4, 5},
		},
		GetErrorMap: &GetErrorMapRequest{Version: 2},
		Auth: &SaslAuthAutoOptions{
			EnabledMechs: []AuthMechanism{PlainAuthMechanism},
			Username:     "dave",
			Password:     "asecretdontlook",
		},
		SelectBucket:     &SelectBucketRequest{BucketName: "bucket"},
		GetClusterConfig: &GetClusterConfigRequest{},
	}

	return opts
}

func makeDefaultTestBootstrapEncoder(errmap []byte, cfg []byte) *testOpBootstrapEncoder {
	enc := &testOpBootstrapEncoder{
		Responses: testOpBootstrapEncoderResponses{
			Hello: unaryResult[*HelloResponse]{
				Resp: &HelloResponse{
					EnabledFeatures: []HelloFeature{1, 2, 3, 4, 5},
				},
			},
			GetErrorMap: unaryResult[[]byte]{
				Resp: errmap,
			},
			SASLAuth: unaryResult[*SASLAuthResponse]{
				Resp: &SASLAuthResponse{
					NeedsMoreSteps: false,
				},
			},
			SASLStep: unaryResult[*SASLStepResponse]{},
			SASLListMech: unaryResult[*SASLListMechsResponse]{
				Resp: &SASLListMechsResponse{AvailableMechs: []AuthMechanism{PlainAuthMechanism}},
			},
			SelectBucket: nil,
			GetClusterConfig: unaryResult[[]byte]{
				Resp: cfg,
			},
		},
		helloDoneCh:    make(chan struct{}),
		errMapDoneCh:   make(chan struct{}),
		saslListDoneCh: make(chan struct{}),
		saslAuthDoneCh: make(chan struct{}),
		selectBucketCh: make(chan struct{}),
	}

	return enc
}

// testBootstrapDispatcher doesn't actually do anything.
type testBootstrapDispatcher struct {
	packets []Packet
}

func (t testBootstrapDispatcher) Dispatch(packet *Packet, callback DispatchCallback) (PendingOp, error) {
	return pendingOpNoop{}, nil
}

func (t testBootstrapDispatcher) LocalAddr() string {
	return "localaddr"
}

func (t testBootstrapDispatcher) RemoteAddr() string {
	return "remoteaddr"
}

type testOpBootstrapEncoderRequests struct {
	Hello            []*HelloRequest
	GetErrorMap      []*GetErrorMapRequest
	SASLAuth         []*SASLAuthRequest
	SASLStep         []*SASLStepRequest
	SASLListMech     int
	SelectBucket     []*SelectBucketRequest
	GetClusterConfig []*GetClusterConfigRequest
}

type testOpBootstrapEncoderResponses struct {
	Hello            unaryResult[*HelloResponse]
	GetErrorMap      unaryResult[[]byte]
	SASLAuth         unaryResult[*SASLAuthResponse]
	SASLStep         unaryResult[*SASLStepResponse]
	SASLListMech     unaryResult[*SASLListMechsResponse]
	SelectBucket     error
	GetClusterConfig unaryResult[[]byte]
}

type testOpBootstrapEncoderDispatchErrors struct {
	Hello            error
	GetErrorMap      error
	SASLAuth         error
	SASLStep         error
	SASLListMech     error
	SelectBucket     error
	GetClusterConfig error
}

type bootstrapCoordinationPair struct {
	Reached  chan struct{}
	Continue chan struct{}
}

type bootstrapPendingOp[T any] struct {
	cb func(T, error)
}

func (op *bootstrapPendingOp[T]) Cancel(err error) {
	var emptyResp T
	op.cb(emptyResp, err)
}

type testOpBootstrapEncoder struct {
	Requests testOpBootstrapEncoderRequests

	Responses      testOpBootstrapEncoderResponses
	DispatchErrors testOpBootstrapEncoderDispatchErrors

	// Bootstrap requests are guaranteed to respond in the order that they are sent.
	// TODO(chvck): Add tests to simulate one of the packets gets dropped on the network.
	helloDoneCh    chan struct{}
	errMapDoneCh   chan struct{}
	saslListDoneCh chan struct{}
	saslAuthDoneCh chan struct{}
	selectBucketCh chan struct{}

	HelloBlockCh            *bootstrapCoordinationPair
	ErrMapBlockCh           *bootstrapCoordinationPair
	SASLListBlockCh         *bootstrapCoordinationPair
	SASLAuthBlockCh         *bootstrapCoordinationPair
	SelectBucketBlockCh     *bootstrapCoordinationPair
	GetClusterConfigBlockCh *bootstrapCoordinationPair
}

func (t *testOpBootstrapEncoder) Hello(dispatcher Dispatcher, request *HelloRequest, f func(*HelloResponse, error)) (PendingOp, error) {
	t.Requests.Hello = append(t.Requests.Hello, request)

	if t.DispatchErrors.Hello != nil {
		return nil, t.DispatchErrors.Hello
	}

	go func() {
		if t.HelloBlockCh != nil {
			t.HelloBlockCh.Reached <- struct{}{}
			<-t.HelloBlockCh.Continue
		}
		f(t.Responses.Hello.Resp, t.Responses.Hello.Err)
		t.helloDoneCh <- struct{}{}
	}()

	return &bootstrapPendingOp[*HelloResponse]{
		cb: f,
	}, nil
}

func (t *testOpBootstrapEncoder) GetErrorMap(dispatcher Dispatcher, request *GetErrorMapRequest, f func([]byte, error)) (PendingOp, error) {
	t.Requests.GetErrorMap = append(t.Requests.GetErrorMap, request)

	if t.DispatchErrors.GetErrorMap != nil {
		return nil, t.DispatchErrors.GetErrorMap
	}

	go func() {
		<-t.helloDoneCh
		if t.ErrMapBlockCh != nil {
			t.ErrMapBlockCh.Reached <- struct{}{}
			<-t.ErrMapBlockCh.Continue
		}
		f(t.Responses.GetErrorMap.Resp, t.Responses.GetErrorMap.Err)
		t.errMapDoneCh <- struct{}{}
	}()

	return &bootstrapPendingOp[[]byte]{
		cb: f,
	}, nil
}

func (t *testOpBootstrapEncoder) SASLAuth(dispatcher Dispatcher, request *SASLAuthRequest, f func(*SASLAuthResponse, error)) (PendingOp, error) {
	t.Requests.SASLAuth = append(t.Requests.SASLAuth, request)

	if t.DispatchErrors.SASLAuth != nil {
		return nil, t.DispatchErrors.SASLAuth
	}

	go func() {
		<-t.saslListDoneCh
		if t.SASLAuthBlockCh != nil {
			t.SASLAuthBlockCh.Reached <- struct{}{}
			<-t.SASLAuthBlockCh.Continue
		}
		f(t.Responses.SASLAuth.Resp, t.Responses.SASLAuth.Err)
		t.saslAuthDoneCh <- struct{}{}
	}()

	return &bootstrapPendingOp[*SASLAuthResponse]{
		cb: f,
	}, nil
}

func (t *testOpBootstrapEncoder) SASLStep(dispatcher Dispatcher, request *SASLStepRequest, f func(*SASLStepResponse, error)) (PendingOp, error) {
	panic("not implemented")
}

func (t *testOpBootstrapEncoder) SASLListMechs(dispatcher Dispatcher, f func(*SASLListMechsResponse, error)) (PendingOp, error) {
	t.Requests.SASLListMech++

	if t.DispatchErrors.SASLListMech != nil {
		return nil, t.DispatchErrors.SASLListMech
	}

	go func() {
		<-t.errMapDoneCh
		if t.SASLListBlockCh != nil {
			t.SASLListBlockCh.Reached <- struct{}{}
			<-t.SASLListBlockCh.Continue
		}
		f(t.Responses.SASLListMech.Resp, t.Responses.SASLListMech.Err)
		t.saslListDoneCh <- struct{}{}
	}()

	return &bootstrapPendingOp[*SASLListMechsResponse]{
		cb: f,
	}, nil
}

func (t *testOpBootstrapEncoder) SelectBucket(dispatcher Dispatcher, request *SelectBucketRequest, f func(error)) (PendingOp, error) {
	t.Requests.SelectBucket = append(t.Requests.SelectBucket, request)

	if t.DispatchErrors.SelectBucket != nil {
		return nil, t.DispatchErrors.SelectBucket
	}

	go func() {
		<-t.saslAuthDoneCh
		if t.SelectBucketBlockCh != nil {
			t.SelectBucketBlockCh.Reached <- struct{}{}
			<-t.SelectBucketBlockCh.Continue
		}
		f(t.Responses.SelectBucket)
		t.selectBucketCh <- struct{}{}
	}()

	wrapper := func(_ struct{}, err error) {
		f(err)
	}

	return &bootstrapPendingOp[struct{}]{
		cb: wrapper,
	}, nil
}

func (t *testOpBootstrapEncoder) GetClusterConfig(dispatcher Dispatcher, request *GetClusterConfigRequest, f func([]byte, error)) (PendingOp, error) {
	t.Requests.GetClusterConfig = append(t.Requests.GetClusterConfig, request)

	if t.DispatchErrors.GetClusterConfig != nil {
		return nil, t.DispatchErrors.GetClusterConfig
	}

	go func() {
		<-t.selectBucketCh
		if t.GetClusterConfigBlockCh != nil {
			t.GetClusterConfigBlockCh.Reached <- struct{}{}
			<-t.GetClusterConfigBlockCh.Continue
		}
		f(t.Responses.GetClusterConfig.Resp, t.Responses.GetClusterConfig.Err)
	}()

	return &bootstrapPendingOp[[]byte]{
		cb: f,
	}, nil
}
