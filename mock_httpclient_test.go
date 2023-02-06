// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package core

import (
	"context"
	"sync"
)

// Ensure, that HTTPClientMock does implement HTTPClient.
// If this is not the case, regenerate this file with moq.
var _ HTTPClient = &HTTPClientMock{}

// HTTPClientMock is a mock implementation of HTTPClient.
//
//	func TestSomethingThatUsesHTTPClient(t *testing.T) {
//
//		// make and configure a mocked HTTPClient
//		mockedHTTPClient := &HTTPClientMock{
//			CloseFunc: func() error {
//				panic("mock out the Close method")
//			},
//			DoFunc: func(ctx context.Context, request *HTTPRequest) (*HTTPResponse, error) {
//				panic("mock out the Do method")
//			},
//			ManagementEndpointsFunc: func() []string {
//				panic("mock out the ManagementEndpoints method")
//			},
//			ReconfigureFunc: func(config *HTTPClientConfig) error {
//				panic("mock out the Reconfigure method")
//			},
//		}
//
//		// use mockedHTTPClient in code that requires HTTPClient
//		// and then make assertions.
//
//	}
type HTTPClientMock struct {
	// CloseFunc mocks the Close method.
	CloseFunc func() error

	// DoFunc mocks the Do method.
	DoFunc func(ctx context.Context, request *HTTPRequest) (*HTTPResponse, error)

	// ManagementEndpointsFunc mocks the ManagementEndpoints method.
	ManagementEndpointsFunc func() []string

	// ReconfigureFunc mocks the Reconfigure method.
	ReconfigureFunc func(config *HTTPClientConfig) error

	// calls tracks calls to the methods.
	calls struct {
		// Close holds details about calls to the Close method.
		Close []struct {
		}
		// Do holds details about calls to the Do method.
		Do []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// Request is the request argument value.
			Request *HTTPRequest
		}
		// ManagementEndpoints holds details about calls to the ManagementEndpoints method.
		ManagementEndpoints []struct {
		}
		// Reconfigure holds details about calls to the Reconfigure method.
		Reconfigure []struct {
			// Config is the config argument value.
			Config *HTTPClientConfig
		}
	}
	lockClose               sync.RWMutex
	lockDo                  sync.RWMutex
	lockManagementEndpoints sync.RWMutex
	lockReconfigure         sync.RWMutex
}

// Close calls CloseFunc.
func (mock *HTTPClientMock) Close() error {
	if mock.CloseFunc == nil {
		panic("HTTPClientMock.CloseFunc: method is nil but HTTPClient.Close was just called")
	}
	callInfo := struct {
	}{}
	mock.lockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	mock.lockClose.Unlock()
	return mock.CloseFunc()
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//
//	len(mockedHTTPClient.CloseCalls())
func (mock *HTTPClientMock) CloseCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockClose.RLock()
	calls = mock.calls.Close
	mock.lockClose.RUnlock()
	return calls
}

// Do calls DoFunc.
func (mock *HTTPClientMock) Do(ctx context.Context, request *HTTPRequest) (*HTTPResponse, error) {
	if mock.DoFunc == nil {
		panic("HTTPClientMock.DoFunc: method is nil but HTTPClient.Do was just called")
	}
	callInfo := struct {
		Ctx     context.Context
		Request *HTTPRequest
	}{
		Ctx:     ctx,
		Request: request,
	}
	mock.lockDo.Lock()
	mock.calls.Do = append(mock.calls.Do, callInfo)
	mock.lockDo.Unlock()
	return mock.DoFunc(ctx, request)
}

// DoCalls gets all the calls that were made to Do.
// Check the length with:
//
//	len(mockedHTTPClient.DoCalls())
func (mock *HTTPClientMock) DoCalls() []struct {
	Ctx     context.Context
	Request *HTTPRequest
} {
	var calls []struct {
		Ctx     context.Context
		Request *HTTPRequest
	}
	mock.lockDo.RLock()
	calls = mock.calls.Do
	mock.lockDo.RUnlock()
	return calls
}

// ManagementEndpoints calls ManagementEndpointsFunc.
func (mock *HTTPClientMock) ManagementEndpoints() []string {
	if mock.ManagementEndpointsFunc == nil {
		panic("HTTPClientMock.ManagementEndpointsFunc: method is nil but HTTPClient.ManagementEndpoints was just called")
	}
	callInfo := struct {
	}{}
	mock.lockManagementEndpoints.Lock()
	mock.calls.ManagementEndpoints = append(mock.calls.ManagementEndpoints, callInfo)
	mock.lockManagementEndpoints.Unlock()
	return mock.ManagementEndpointsFunc()
}

// ManagementEndpointsCalls gets all the calls that were made to ManagementEndpoints.
// Check the length with:
//
//	len(mockedHTTPClient.ManagementEndpointsCalls())
func (mock *HTTPClientMock) ManagementEndpointsCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockManagementEndpoints.RLock()
	calls = mock.calls.ManagementEndpoints
	mock.lockManagementEndpoints.RUnlock()
	return calls
}

// Reconfigure calls ReconfigureFunc.
func (mock *HTTPClientMock) Reconfigure(config *HTTPClientConfig) error {
	if mock.ReconfigureFunc == nil {
		panic("HTTPClientMock.ReconfigureFunc: method is nil but HTTPClient.Reconfigure was just called")
	}
	callInfo := struct {
		Config *HTTPClientConfig
	}{
		Config: config,
	}
	mock.lockReconfigure.Lock()
	mock.calls.Reconfigure = append(mock.calls.Reconfigure, callInfo)
	mock.lockReconfigure.Unlock()
	return mock.ReconfigureFunc(config)
}

// ReconfigureCalls gets all the calls that were made to Reconfigure.
// Check the length with:
//
//	len(mockedHTTPClient.ReconfigureCalls())
func (mock *HTTPClientMock) ReconfigureCalls() []struct {
	Config *HTTPClientConfig
} {
	var calls []struct {
		Config *HTTPClientConfig
	}
	mock.lockReconfigure.RLock()
	calls = mock.calls.Reconfigure
	mock.lockReconfigure.RUnlock()
	return calls
}