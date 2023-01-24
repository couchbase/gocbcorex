// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package core

import (
	"context"
	"sync"
)

// Ensure, that CollectionResolverMock does implement CollectionResolver.
// If this is not the case, regenerate this file with moq.
var _ CollectionResolver = &CollectionResolverMock{}

// CollectionResolverMock is a mock implementation of CollectionResolver.
//
//	func TestSomethingThatUsesCollectionResolver(t *testing.T) {
//
//		// make and configure a mocked CollectionResolver
//		mockedCollectionResolver := &CollectionResolverMock{
//			InvalidateCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string, endpoint string, manifestRev uint64)  {
//				panic("mock out the InvalidateCollectionID method")
//			},
//			ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
//				panic("mock out the ResolveCollectionID method")
//			},
//		}
//
//		// use mockedCollectionResolver in code that requires CollectionResolver
//		// and then make assertions.
//
//	}
type CollectionResolverMock struct {
	// InvalidateCollectionIDFunc mocks the InvalidateCollectionID method.
	InvalidateCollectionIDFunc func(ctx context.Context, scopeName string, collectionName string, endpoint string, manifestRev uint64)

	// ResolveCollectionIDFunc mocks the ResolveCollectionID method.
	ResolveCollectionIDFunc func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error)

	// calls tracks calls to the methods.
	calls struct {
		// InvalidateCollectionID holds details about calls to the InvalidateCollectionID method.
		InvalidateCollectionID []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// ScopeName is the scopeName argument value.
			ScopeName string
			// CollectionName is the collectionName argument value.
			CollectionName string
			// Endpoint is the endpoint argument value.
			Endpoint string
			// ManifestRev is the manifestRev argument value.
			ManifestRev uint64
		}
		// ResolveCollectionID holds details about calls to the ResolveCollectionID method.
		ResolveCollectionID []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// ScopeName is the scopeName argument value.
			ScopeName string
			// CollectionName is the collectionName argument value.
			CollectionName string
		}
	}
	lockInvalidateCollectionID sync.RWMutex
	lockResolveCollectionID    sync.RWMutex
}

// InvalidateCollectionID calls InvalidateCollectionIDFunc.
func (mock *CollectionResolverMock) InvalidateCollectionID(ctx context.Context, scopeName string, collectionName string, endpoint string, manifestRev uint64) {
	if mock.InvalidateCollectionIDFunc == nil {
		panic("CollectionResolverMock.InvalidateCollectionIDFunc: method is nil but CollectionResolver.InvalidateCollectionID was just called")
	}
	callInfo := struct {
		Ctx            context.Context
		ScopeName      string
		CollectionName string
		Endpoint       string
		ManifestRev    uint64
	}{
		Ctx:            ctx,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		Endpoint:       endpoint,
		ManifestRev:    manifestRev,
	}
	mock.lockInvalidateCollectionID.Lock()
	mock.calls.InvalidateCollectionID = append(mock.calls.InvalidateCollectionID, callInfo)
	mock.lockInvalidateCollectionID.Unlock()
	mock.InvalidateCollectionIDFunc(ctx, scopeName, collectionName, endpoint, manifestRev)
}

// InvalidateCollectionIDCalls gets all the calls that were made to InvalidateCollectionID.
// Check the length with:
//
//	len(mockedCollectionResolver.InvalidateCollectionIDCalls())
func (mock *CollectionResolverMock) InvalidateCollectionIDCalls() []struct {
	Ctx            context.Context
	ScopeName      string
	CollectionName string
	Endpoint       string
	ManifestRev    uint64
} {
	var calls []struct {
		Ctx            context.Context
		ScopeName      string
		CollectionName string
		Endpoint       string
		ManifestRev    uint64
	}
	mock.lockInvalidateCollectionID.RLock()
	calls = mock.calls.InvalidateCollectionID
	mock.lockInvalidateCollectionID.RUnlock()
	return calls
}

// ResolveCollectionID calls ResolveCollectionIDFunc.
func (mock *CollectionResolverMock) ResolveCollectionID(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
	if mock.ResolveCollectionIDFunc == nil {
		panic("CollectionResolverMock.ResolveCollectionIDFunc: method is nil but CollectionResolver.ResolveCollectionID was just called")
	}
	callInfo := struct {
		Ctx            context.Context
		ScopeName      string
		CollectionName string
	}{
		Ctx:            ctx,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}
	mock.lockResolveCollectionID.Lock()
	mock.calls.ResolveCollectionID = append(mock.calls.ResolveCollectionID, callInfo)
	mock.lockResolveCollectionID.Unlock()
	return mock.ResolveCollectionIDFunc(ctx, scopeName, collectionName)
}

// ResolveCollectionIDCalls gets all the calls that were made to ResolveCollectionID.
// Check the length with:
//
//	len(mockedCollectionResolver.ResolveCollectionIDCalls())
func (mock *CollectionResolverMock) ResolveCollectionIDCalls() []struct {
	Ctx            context.Context
	ScopeName      string
	CollectionName string
} {
	var calls []struct {
		Ctx            context.Context
		ScopeName      string
		CollectionName string
	}
	mock.lockResolveCollectionID.RLock()
	calls = mock.calls.ResolveCollectionID
	mock.lockResolveCollectionID.RUnlock()
	return calls
}