// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package core

import (
	"context"
	"sync"
)

// Ensure, that VbucketRouterMock does implement VbucketRouter.
// If this is not the case, regenerate this file with moq.
var _ VbucketRouter = &VbucketRouterMock{}

// VbucketRouterMock is a mock implementation of VbucketRouter.
//
//	func TestSomethingThatUsesVbucketRouter(t *testing.T) {
//
//		// make and configure a mocked VbucketRouter
//		mockedVbucketRouter := &VbucketRouterMock{
//			DispatchByKeyFunc: func(ctx context.Context, key []byte) (string, uint16, error) {
//				panic("mock out the DispatchByKey method")
//			},
//			DispatchToVbucketFunc: func(ctx context.Context, vbID uint16) (string, error) {
//				panic("mock out the DispatchToVbucket method")
//			},
//		}
//
//		// use mockedVbucketRouter in code that requires VbucketRouter
//		// and then make assertions.
//
//	}
type VbucketRouterMock struct {
	// DispatchByKeyFunc mocks the DispatchByKey method.
	DispatchByKeyFunc func(ctx context.Context, key []byte) (string, uint16, error)

	// DispatchToVbucketFunc mocks the DispatchToVbucket method.
	DispatchToVbucketFunc func(ctx context.Context, vbID uint16) (string, error)

	// calls tracks calls to the methods.
	calls struct {
		// DispatchByKey holds details about calls to the DispatchByKey method.
		DispatchByKey []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// Key is the key argument value.
			Key []byte
		}
		// DispatchToVbucket holds details about calls to the DispatchToVbucket method.
		DispatchToVbucket []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// VbID is the vbID argument value.
			VbID uint16
		}
	}
	lockDispatchByKey     sync.RWMutex
	lockDispatchToVbucket sync.RWMutex
}

// DispatchByKey calls DispatchByKeyFunc.
func (mock *VbucketRouterMock) DispatchByKey(ctx context.Context, key []byte) (string, uint16, error) {
	if mock.DispatchByKeyFunc == nil {
		panic("VbucketRouterMock.DispatchByKeyFunc: method is nil but VbucketRouter.DispatchByKey was just called")
	}
	callInfo := struct {
		Ctx context.Context
		Key []byte
	}{
		Ctx: ctx,
		Key: key,
	}
	mock.lockDispatchByKey.Lock()
	mock.calls.DispatchByKey = append(mock.calls.DispatchByKey, callInfo)
	mock.lockDispatchByKey.Unlock()
	return mock.DispatchByKeyFunc(ctx, key)
}

// DispatchByKeyCalls gets all the calls that were made to DispatchByKey.
// Check the length with:
//
//	len(mockedVbucketRouter.DispatchByKeyCalls())
func (mock *VbucketRouterMock) DispatchByKeyCalls() []struct {
	Ctx context.Context
	Key []byte
} {
	var calls []struct {
		Ctx context.Context
		Key []byte
	}
	mock.lockDispatchByKey.RLock()
	calls = mock.calls.DispatchByKey
	mock.lockDispatchByKey.RUnlock()
	return calls
}

// DispatchToVbucket calls DispatchToVbucketFunc.
func (mock *VbucketRouterMock) DispatchToVbucket(ctx context.Context, vbID uint16) (string, error) {
	if mock.DispatchToVbucketFunc == nil {
		panic("VbucketRouterMock.DispatchToVbucketFunc: method is nil but VbucketRouter.DispatchToVbucket was just called")
	}
	callInfo := struct {
		Ctx  context.Context
		VbID uint16
	}{
		Ctx:  ctx,
		VbID: vbID,
	}
	mock.lockDispatchToVbucket.Lock()
	mock.calls.DispatchToVbucket = append(mock.calls.DispatchToVbucket, callInfo)
	mock.lockDispatchToVbucket.Unlock()
	return mock.DispatchToVbucketFunc(ctx, vbID)
}

// DispatchToVbucketCalls gets all the calls that were made to DispatchToVbucket.
// Check the length with:
//
//	len(mockedVbucketRouter.DispatchToVbucketCalls())
func (mock *VbucketRouterMock) DispatchToVbucketCalls() []struct {
	Ctx  context.Context
	VbID uint16
} {
	var calls []struct {
		Ctx  context.Context
		VbID uint16
	}
	mock.lockDispatchToVbucket.RLock()
	calls = mock.calls.DispatchToVbucket
	mock.lockDispatchToVbucket.RUnlock()
	return calls
}