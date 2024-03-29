// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package cbauthx

import (
	"context"
	"sync"
)

// Ensure, that AuthCheckMock does implement AuthCheck.
// If this is not the case, regenerate this file with moq.
var _ AuthCheck = &AuthCheckMock{}

// AuthCheckMock is a mock implementation of AuthCheck.
//
//	func TestSomethingThatUsesAuthCheck(t *testing.T) {
//
//		// make and configure a mocked AuthCheck
//		mockedAuthCheck := &AuthCheckMock{
//			CheckUserPassFunc: func(ctx context.Context, username string, password string) (UserInfo, error) {
//				panic("mock out the CheckUserPass method")
//			},
//		}
//
//		// use mockedAuthCheck in code that requires AuthCheck
//		// and then make assertions.
//
//	}
type AuthCheckMock struct {
	// CheckUserPassFunc mocks the CheckUserPass method.
	CheckUserPassFunc func(ctx context.Context, username string, password string) (UserInfo, error)

	// calls tracks calls to the methods.
	calls struct {
		// CheckUserPass holds details about calls to the CheckUserPass method.
		CheckUserPass []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// Username is the username argument value.
			Username string
			// Password is the password argument value.
			Password string
		}
	}
	lockCheckUserPass sync.RWMutex
}

// CheckUserPass calls CheckUserPassFunc.
func (mock *AuthCheckMock) CheckUserPass(ctx context.Context, username string, password string) (UserInfo, error) {
	if mock.CheckUserPassFunc == nil {
		panic("AuthCheckMock.CheckUserPassFunc: method is nil but AuthCheck.CheckUserPass was just called")
	}
	callInfo := struct {
		Ctx      context.Context
		Username string
		Password string
	}{
		Ctx:      ctx,
		Username: username,
		Password: password,
	}
	mock.lockCheckUserPass.Lock()
	mock.calls.CheckUserPass = append(mock.calls.CheckUserPass, callInfo)
	mock.lockCheckUserPass.Unlock()
	return mock.CheckUserPassFunc(ctx, username, password)
}

// CheckUserPassCalls gets all the calls that were made to CheckUserPass.
// Check the length with:
//
//	len(mockedAuthCheck.CheckUserPassCalls())
func (mock *AuthCheckMock) CheckUserPassCalls() []struct {
	Ctx      context.Context
	Username string
	Password string
} {
	var calls []struct {
		Ctx      context.Context
		Username string
		Password string
	}
	mock.lockCheckUserPass.RLock()
	calls = mock.calls.CheckUserPass
	mock.lockCheckUserPass.RUnlock()
	return calls
}
