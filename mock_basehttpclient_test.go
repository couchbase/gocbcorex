// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package core

import (
	"net/http"
	"sync"
)

// Ensure, that BaseHTTPClientMock does implement BaseHTTPClient.
// If this is not the case, regenerate this file with moq.
var _ BaseHTTPClient = &BaseHTTPClientMock{}

// BaseHTTPClientMock is a mock implementation of BaseHTTPClient.
//
//	func TestSomethingThatUsesBaseHTTPClient(t *testing.T) {
//
//		// make and configure a mocked BaseHTTPClient
//		mockedBaseHTTPClient := &BaseHTTPClientMock{
//			CloseIdleConnectionsFunc: func()  {
//				panic("mock out the CloseIdleConnections method")
//			},
//			DoFunc: func(r *http.Request) (*http.Response, error) {
//				panic("mock out the Do method")
//			},
//		}
//
//		// use mockedBaseHTTPClient in code that requires BaseHTTPClient
//		// and then make assertions.
//
//	}
type BaseHTTPClientMock struct {
	// CloseIdleConnectionsFunc mocks the CloseIdleConnections method.
	CloseIdleConnectionsFunc func()

	// DoFunc mocks the Do method.
	DoFunc func(r *http.Request) (*http.Response, error)

	// calls tracks calls to the methods.
	calls struct {
		// CloseIdleConnections holds details about calls to the CloseIdleConnections method.
		CloseIdleConnections []struct {
		}
		// Do holds details about calls to the Do method.
		Do []struct {
			// R is the r argument value.
			R *http.Request
		}
	}
	lockCloseIdleConnections sync.RWMutex
	lockDo                   sync.RWMutex
}

// CloseIdleConnections calls CloseIdleConnectionsFunc.
func (mock *BaseHTTPClientMock) CloseIdleConnections() {
	if mock.CloseIdleConnectionsFunc == nil {
		panic("BaseHTTPClientMock.CloseIdleConnectionsFunc: method is nil but BaseHTTPClient.CloseIdleConnections was just called")
	}
	callInfo := struct {
	}{}
	mock.lockCloseIdleConnections.Lock()
	mock.calls.CloseIdleConnections = append(mock.calls.CloseIdleConnections, callInfo)
	mock.lockCloseIdleConnections.Unlock()
	mock.CloseIdleConnectionsFunc()
}

// CloseIdleConnectionsCalls gets all the calls that were made to CloseIdleConnections.
// Check the length with:
//
//	len(mockedBaseHTTPClient.CloseIdleConnectionsCalls())
func (mock *BaseHTTPClientMock) CloseIdleConnectionsCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockCloseIdleConnections.RLock()
	calls = mock.calls.CloseIdleConnections
	mock.lockCloseIdleConnections.RUnlock()
	return calls
}

// Do calls DoFunc.
func (mock *BaseHTTPClientMock) Do(r *http.Request) (*http.Response, error) {
	if mock.DoFunc == nil {
		panic("BaseHTTPClientMock.DoFunc: method is nil but BaseHTTPClient.Do was just called")
	}
	callInfo := struct {
		R *http.Request
	}{
		R: r,
	}
	mock.lockDo.Lock()
	mock.calls.Do = append(mock.calls.Do, callInfo)
	mock.lockDo.Unlock()
	return mock.DoFunc(r)
}

// DoCalls gets all the calls that were made to Do.
// Check the length with:
//
//	len(mockedBaseHTTPClient.DoCalls())
func (mock *BaseHTTPClientMock) DoCalls() []struct {
	R *http.Request
} {
	var calls []struct {
		R *http.Request
	}
	mock.lockDo.RLock()
	calls = mock.calls.Do
	mock.lockDo.RUnlock()
	return calls
}