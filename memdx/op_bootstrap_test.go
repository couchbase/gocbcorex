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

	res, err := SyncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	require.NoError(t, err)

	assert.Equal(t, enc.Responses.Hello.Resp.EnabledFeatures, res.Hello.EnabledFeatures)
	assert.Equal(t, errmap, res.ErrorMap.ErrorMap)
	assert.Equal(t, cfg, res.ClusterConfig.Config)
}

func TestOpBootstrapHelloFails(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.Responses.Hello = UnaryResult[*HelloResponse]{
		Err: errors.New("i failed"),
	}

	opts := makeDefaultBootstrapOptions()

	res, err := SyncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	require.NoError(t, err)

	assert.Nil(t, res.Hello)
	assert.Equal(t, errmap, res.ErrorMap.ErrorMap)
	assert.Equal(t, cfg, res.ClusterConfig.Config)
}

func TestOpBootstrapGetErrMapFails(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.Responses.GetErrorMap = UnaryResult[*GetErrorMapResponse]{
		Err: errors.New("i failed"),
	}

	opts := makeDefaultBootstrapOptions()

	res, err := SyncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	require.NoError(t, err)

	assert.Equal(t, enc.Responses.Hello.Resp.EnabledFeatures, res.Hello.EnabledFeatures)
	assert.Nil(t, res.ErrorMap)
	assert.Equal(t, cfg, res.ClusterConfig.Config)
}

func TestOpBootstrapListMechFails(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.Responses.SASLListMech = UnaryResult[*SASLListMechsResponse]{
		Err: errors.New("error"),
	}

	opts := makeDefaultBootstrapOptions()

	res, err := SyncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	require.NoError(t, err)

	assert.Equal(t, enc.Responses.Hello.Resp.EnabledFeatures, res.Hello.EnabledFeatures)
	assert.Equal(t, errmap, res.ErrorMap.ErrorMap)
	assert.Equal(t, cfg, res.ClusterConfig.Config)
}

func TestOpBootstrapAuthFails(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.Responses.SASLAuth = UnaryResult[*SASLAuthResponse]{
		Err: ErrAuthError,
	}

	opts := makeDefaultBootstrapOptions()

	_, err := SyncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	assert.ErrorIs(t, err, ErrAuthError)
}

func TestOpBootstrapSelectBucketFails(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.Responses.SelectBucket = UnaryResult[*SelectBucketResponse]{
		Err: errors.New("imnobucket"),
	}

	opts := makeDefaultBootstrapOptions()

	_, err := SyncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	assert.ErrorIs(t, err, enc.Responses.SelectBucket.Err)
}

func TestOpBootstrapGetClusterConfigFails(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	enc := makeDefaultTestBootstrapEncoder(errmap, cfg)
	enc.Responses.GetClusterConfig = UnaryResult[*GetClusterConfigResponse]{
		Err: errors.New("i failed"),
	}

	opts := makeDefaultBootstrapOptions()

	res, err := SyncUnaryCall(OpBootstrap{
		Encoder: enc,
	}, OpBootstrap.Bootstrap, testBootstrapDispatcher{}, opts)
	require.NoError(t, err)

	assert.Equal(t, enc.Responses.Hello.Resp.EnabledFeatures, res.Hello.EnabledFeatures)
	assert.Equal(t, errmap, res.ErrorMap.ErrorMap)
	assert.Nil(t, res.ClusterConfig)
}

func TestOpBootstrapCancelled(t *testing.T) {
	errmap := testutils.LoadTestData(t, "err_map71_v2.json")
	cfg := testutils.LoadTestData(t, "bucket_config_with_external_addresses.json")

	opts := makeDefaultBootstrapOptions()

	type test struct {
		Name            string
		TargetFieldName string
		ShouldFail      bool
	}

	tests := []test{
		{
			Name:            "Hello",
			TargetFieldName: "HelloBlockCh",
			ShouldFail:      true,
		},
		{
			Name:            "ErrMap",
			TargetFieldName: "ErrMapBlockCh",
			ShouldFail:      true,
		},
		{
			Name:            "SASLListMechs",
			TargetFieldName: "SASLListBlockCh",
			ShouldFail:      true,
		},
		{
			Name:            "SASLAuth",
			TargetFieldName: "SASLAuthBlockCh",
			ShouldFail:      true,
		},
		{
			Name:            "SelectBucket",
			TargetFieldName: "SelectBucketBlockCh",
			ShouldFail:      true,
		},
		{
			Name:            "GetClusterConfig",
			TargetFieldName: "GetClusterConfigBlockCh",
			ShouldFail:      false,
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

			wait := make(chan UnaryResult[*BootstrapResult], 1)
			op, err := OpBootstrap{
				Encoder: enc,
			}.Bootstrap(testBootstrapDispatcher{}, opts, func(res *BootstrapResult, err error) {
				wait <- UnaryResult[*BootstrapResult]{
					Resp: res,
					Err:  err,
				}
			})
			require.NoError(tt, err)

			expectedErr := errors.New("some error")
			<-pair.Reached
			op.Cancel(&requestCancelledError{expectedErr})
			pair.Continue <- struct{}{}

			// Due to the nature of bootstrap having some optional components which
			// are permitted to fail, some cancellations involving these operations
			// will not normally yield an errored bootstrap.  The one caveat to this
			// is that the second-last operation (SelectBucket) is required, and thus
			// even a cancellation ignored at an earlier stage will lead to the whole
			// bootstrap failing.  In the case of the final GetClusterConfig operation,
			// this does not happen and thus we need to ignore it's error.
			if test.ShouldFail {
				res := <-wait
				assert.ErrorIs(tt, res.Err, expectedErr)
			} else {
				<-wait
			}
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
			Hello: UnaryResult[*HelloResponse]{
				Resp: &HelloResponse{
					EnabledFeatures: []HelloFeature{1, 2, 3, 4, 5},
				},
			},
			GetErrorMap: UnaryResult[*GetErrorMapResponse]{
				Resp: &GetErrorMapResponse{
					ErrorMap: errmap,
				},
			},
			SASLAuth: UnaryResult[*SASLAuthResponse]{
				Resp: &SASLAuthResponse{
					NeedsMoreSteps: false,
				},
			},
			SASLStep: UnaryResult[*SASLStepResponse]{},
			SASLListMech: UnaryResult[*SASLListMechsResponse]{
				Resp: &SASLListMechsResponse{
					AvailableMechs: []AuthMechanism{PlainAuthMechanism},
				},
			},
			SelectBucket: UnaryResult[*SelectBucketResponse]{},
			GetClusterConfig: UnaryResult[*GetClusterConfigResponse]{
				Resp: &GetClusterConfigResponse{
					Config: cfg,
				},
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
}

func (t testBootstrapDispatcher) Dispatch(packet *Packet, callback DispatchCallback) (PendingOp, error) {
	return PendingOpNoop{}, nil
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
	Hello            UnaryResult[*HelloResponse]
	GetErrorMap      UnaryResult[*GetErrorMapResponse]
	SASLAuth         UnaryResult[*SASLAuthResponse]
	SASLStep         UnaryResult[*SASLStepResponse]
	SASLListMech     UnaryResult[*SASLListMechsResponse]
	SelectBucket     UnaryResult[*SelectBucketResponse]
	GetClusterConfig UnaryResult[*GetClusterConfigResponse]
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

func (t *testOpBootstrapEncoder) GetErrorMap(dispatcher Dispatcher, request *GetErrorMapRequest, f func(*GetErrorMapResponse, error)) (PendingOp, error) {
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

	return &bootstrapPendingOp[*GetErrorMapResponse]{
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

func (t *testOpBootstrapEncoder) SASLListMechs(dispatcher Dispatcher, request *SASLListMechsRequest, f func(*SASLListMechsResponse, error)) (PendingOp, error) {
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

func (t *testOpBootstrapEncoder) SelectBucket(dispatcher Dispatcher, request *SelectBucketRequest, f func(*SelectBucketResponse, error)) (PendingOp, error) {
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
		f(t.Responses.SelectBucket.Resp, t.Responses.SelectBucket.Err)
		t.selectBucketCh <- struct{}{}
	}()

	return &bootstrapPendingOp[*SelectBucketResponse]{
		cb: f,
	}, nil
}

func (t *testOpBootstrapEncoder) GetClusterConfig(dispatcher Dispatcher, request *GetClusterConfigRequest, f func(*GetClusterConfigResponse, error)) (PendingOp, error) {
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

	return &bootstrapPendingOp[*GetClusterConfigResponse]{
		cb: f,
	}, nil
}
