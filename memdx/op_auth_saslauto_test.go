package memdx

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestOpSaslAuthAuto_Plain(t *testing.T) {
	enc := &testOpSaslAutoEncoder{
		sASLListMech: unaryResult[*SASLListMechsResponse]{
			Resp: &SASLListMechsResponse{[]AuthMechanism{PlainAuthMechanism}},
		},
		sASLAuth: []unaryResult[*SASLAuthResponse]{
			{
				Resp: &SASLAuthResponse{},
			},
		},

		syncCh: make(chan struct{}, 1),
	}

	opts := &SaslAuthAutoOptions{
		Username:     "test",
		Password:     "testsecret",
		EnabledMechs: []AuthMechanism{PlainAuthMechanism},
	}

	var pipelineCalled bool
	waitCh := make(chan error, 1)
	OpSaslAuthAuto{
		Encoder: enc,
	}.SASLAuthAuto(testBootstrapDispatcher{}, opts, func() {
		pipelineCalled = true
	}, func(err error) {
		waitCh <- err
	})

	assert.True(t, pipelineCalled)
	assert.NoError(t, <-waitCh)
}

func TestOpSaslAuthAuto_NoMechs(t *testing.T) {
	enc := &testOpSaslAutoEncoder{}
	opts := &SaslAuthAutoOptions{
		Username:     "test",
		Password:     "testsecret",
		EnabledMechs: []AuthMechanism{},
	}

	_, err := OpSaslAuthAuto{
		Encoder: enc,
	}.SASLAuthAuto(testBootstrapDispatcher{}, opts, func() {
	}, func(err error) {})
	assert.Error(t, err)
}

func TestOpSaslAuthAuto_FirstAuthFails(t *testing.T) {
	// Here we send SASLAuthAuto with 2 mechs enabled. SASLListMechs will respond with only one mech - which matches
	// the second mech provided. We error the first request, the SDK should detect that it's an unsupported mechanism
	// and attempt auth again with the second mech.
	enc := &testOpSaslAutoEncoder{
		sASLListMech: unaryResult[*SASLListMechsResponse]{
			Resp: &SASLListMechsResponse{[]AuthMechanism{PlainAuthMechanism}},
		},
		sASLAuth: []unaryResult[*SASLAuthResponse]{
			{
				Err: errors.New("failedauth"),
			},
			{
				Resp: &SASLAuthResponse{},
			},
		},
		sASLStep: []unaryResult[*SASLStepResponse]{
			{
				Resp: &SASLStepResponse{},
			},
		},

		syncCh: make(chan struct{}, 1),
	}

	opts := &SaslAuthAutoOptions{
		Username:     "test",
		Password:     "testsecret",
		EnabledMechs: []AuthMechanism{ScramSha512AuthMechanism, PlainAuthMechanism},
	}

	waitCh := make(chan error, 1)
	OpSaslAuthAuto{
		Encoder: enc,
	}.SASLAuthAuto(testBootstrapDispatcher{}, opts, func() {
	}, func(err error) {
		waitCh <- err
	})
	require.NoError(t, <-waitCh)

	reqs := enc.AuthRequests
	require.Len(t, reqs, 2)
	assert.Equal(t, ScramSha512AuthMechanism, reqs[0].Mechanism)
	assert.Equal(t, PlainAuthMechanism, reqs[1].Mechanism)
}

func TestOpSaslAuthAuto_FirstAuthFailsNoCompatibleMechs(t *testing.T) {
	// Here we send SASLAuthAuto with 2 mechs enabled. SASLListMechs will respond with only one mech - which doesn't
	// match any requested mech. We error the first request, the SDK should detect that it's an unsupported mechanism
	// and then find that it has no supported mechanisms.
	enc := &testOpSaslAutoEncoder{
		sASLListMech: unaryResult[*SASLListMechsResponse]{
			Resp: &SASLListMechsResponse{[]AuthMechanism{ScramSha1AuthMechanism}},
		},
		sASLAuth: []unaryResult[*SASLAuthResponse]{
			{
				Err: errors.New("failedauth"),
			},
			{
				Resp: &SASLAuthResponse{},
			},
		},
		sASLStep: []unaryResult[*SASLStepResponse]{
			{
				Resp: &SASLStepResponse{},
			},
		},

		syncCh: make(chan struct{}, 1),
	}

	opts := &SaslAuthAutoOptions{
		Username:     "test",
		Password:     "testsecret",
		EnabledMechs: []AuthMechanism{ScramSha512AuthMechanism, PlainAuthMechanism},
	}

	waitCh := make(chan error, 1)
	OpSaslAuthAuto{
		Encoder: enc,
	}.SASLAuthAuto(testBootstrapDispatcher{}, opts, func() {
	}, func(err error) {
		waitCh <- err
	})
	assert.Error(t, <-waitCh)

	reqs := enc.AuthRequests
	require.Len(t, reqs, 1)
	assert.Equal(t, ScramSha512AuthMechanism, reqs[0].Mechanism)
}

func TestOpSaslAuthAuto_FirstAuthFailsListMechsFails(t *testing.T) {
	enc := &testOpSaslAutoEncoder{
		sASLListMech: unaryResult[*SASLListMechsResponse]{
			Err: errors.New("ohnoes"),
		},
		sASLAuth: []unaryResult[*SASLAuthResponse]{
			{
				Err: errors.New("failedauth"),
			},
			{
				Resp: &SASLAuthResponse{},
			},
		},
		sASLStep: []unaryResult[*SASLStepResponse]{
			{
				Resp: &SASLStepResponse{},
			},
		},

		syncCh: make(chan struct{}, 1),
	}

	opts := &SaslAuthAutoOptions{
		Username:     "test",
		Password:     "testsecret",
		EnabledMechs: []AuthMechanism{ScramSha512AuthMechanism, PlainAuthMechanism},
	}

	waitCh := make(chan error, 1)
	OpSaslAuthAuto{
		Encoder: enc,
	}.SASLAuthAuto(testBootstrapDispatcher{}, opts, func() {
	}, func(err error) {
		waitCh <- err
	})
	assert.Error(t, <-waitCh)

	reqs := enc.AuthRequests
	require.Len(t, reqs, 1)
	assert.Equal(t, ScramSha512AuthMechanism, reqs[0].Mechanism)
}

type testOpSaslAutoEncoder struct {
	sASLAuth     []unaryResult[*SASLAuthResponse]
	sASLStep     []unaryResult[*SASLStepResponse]
	sASLListMech unaryResult[*SASLListMechsResponse]
	syncCh       chan struct{}

	AuthRequests []*SASLAuthRequest

	authCount int
	stepCount int
}

func (t *testOpSaslAutoEncoder) SASLStep(dispatcher Dispatcher, request *SASLStepRequest, f func(*SASLStepResponse, error)) (PendingOp, error) {
	t.stepCount++
	go func() {
		<-t.syncCh
		f(t.sASLStep[t.stepCount-1].Resp, t.sASLStep[t.stepCount-1].Err)
		t.syncCh <- struct{}{}
	}()

	return pendingOpNoop{}, nil
}

func (t *testOpSaslAutoEncoder) SASLAuth(dispatcher Dispatcher, request *SASLAuthRequest, f func(*SASLAuthResponse, error)) (PendingOp, error) {
	t.AuthRequests = append(t.AuthRequests, request)
	t.authCount++
	go func() {
		<-t.syncCh
		f(t.sASLAuth[t.authCount-1].Resp, t.sASLAuth[t.authCount-1].Err)
		t.syncCh <- struct{}{}
	}()

	return pendingOpNoop{}, nil
}

func (t *testOpSaslAutoEncoder) SASLListMechs(dispatcher Dispatcher, f func(*SASLListMechsResponse, error)) (PendingOp, error) {
	go func() {
		f(t.sASLListMech.Resp, t.sASLListMech.Err)
		t.syncCh <- struct{}{}
	}()

	return pendingOpNoop{}, nil
}
