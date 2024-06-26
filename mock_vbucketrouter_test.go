// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package gocbcorex

import (
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
//			DispatchByKeyFunc: func(key []byte, vbServerIdx uint32) (string, uint16, error) {
//				panic("mock out the DispatchByKey method")
//			},
//			DispatchToVbucketFunc: func(vbID uint16) (string, error) {
//				panic("mock out the DispatchToVbucket method")
//			},
//			NumReplicasFunc: func() (int, error) {
//				panic("mock out the NumReplicas method")
//			},
//			UpdateRoutingInfoFunc: func(vbucketRoutingInfo *VbucketRoutingInfo)  {
//				panic("mock out the UpdateRoutingInfo method")
//			},
//		}
//
//		// use mockedVbucketRouter in code that requires VbucketRouter
//		// and then make assertions.
//
//	}
type VbucketRouterMock struct {
	// DispatchByKeyFunc mocks the DispatchByKey method.
	DispatchByKeyFunc func(key []byte, vbServerIdx uint32) (string, uint16, error)

	// DispatchToVbucketFunc mocks the DispatchToVbucket method.
	DispatchToVbucketFunc func(vbID uint16) (string, error)

	// NumReplicasFunc mocks the NumReplicas method.
	NumReplicasFunc func() (int, error)

	// UpdateRoutingInfoFunc mocks the UpdateRoutingInfo method.
	UpdateRoutingInfoFunc func(vbucketRoutingInfo *VbucketRoutingInfo)

	// calls tracks calls to the methods.
	calls struct {
		// DispatchByKey holds details about calls to the DispatchByKey method.
		DispatchByKey []struct {
			// Key is the key argument value.
			Key []byte
			// VbServerIdx is the vbServerIdx argument value.
			VbServerIdx uint32
		}
		// DispatchToVbucket holds details about calls to the DispatchToVbucket method.
		DispatchToVbucket []struct {
			// VbID is the vbID argument value.
			VbID uint16
		}
		// NumReplicas holds details about calls to the NumReplicas method.
		NumReplicas []struct {
		}
		// UpdateRoutingInfo holds details about calls to the UpdateRoutingInfo method.
		UpdateRoutingInfo []struct {
			// VbucketRoutingInfo is the vbucketRoutingInfo argument value.
			VbucketRoutingInfo *VbucketRoutingInfo
		}
	}
	lockDispatchByKey     sync.RWMutex
	lockDispatchToVbucket sync.RWMutex
	lockNumReplicas       sync.RWMutex
	lockUpdateRoutingInfo sync.RWMutex
}

// DispatchByKey calls DispatchByKeyFunc.
func (mock *VbucketRouterMock) DispatchByKey(key []byte, vbServerIdx uint32) (string, uint16, error) {
	if mock.DispatchByKeyFunc == nil {
		panic("VbucketRouterMock.DispatchByKeyFunc: method is nil but VbucketRouter.DispatchByKey was just called")
	}
	callInfo := struct {
		Key         []byte
		VbServerIdx uint32
	}{
		Key:         key,
		VbServerIdx: vbServerIdx,
	}
	mock.lockDispatchByKey.Lock()
	mock.calls.DispatchByKey = append(mock.calls.DispatchByKey, callInfo)
	mock.lockDispatchByKey.Unlock()
	return mock.DispatchByKeyFunc(key, vbServerIdx)
}

// DispatchByKeyCalls gets all the calls that were made to DispatchByKey.
// Check the length with:
//
//	len(mockedVbucketRouter.DispatchByKeyCalls())
func (mock *VbucketRouterMock) DispatchByKeyCalls() []struct {
	Key         []byte
	VbServerIdx uint32
} {
	var calls []struct {
		Key         []byte
		VbServerIdx uint32
	}
	mock.lockDispatchByKey.RLock()
	calls = mock.calls.DispatchByKey
	mock.lockDispatchByKey.RUnlock()
	return calls
}

// DispatchToVbucket calls DispatchToVbucketFunc.
func (mock *VbucketRouterMock) DispatchToVbucket(vbID uint16) (string, error) {
	if mock.DispatchToVbucketFunc == nil {
		panic("VbucketRouterMock.DispatchToVbucketFunc: method is nil but VbucketRouter.DispatchToVbucket was just called")
	}
	callInfo := struct {
		VbID uint16
	}{
		VbID: vbID,
	}
	mock.lockDispatchToVbucket.Lock()
	mock.calls.DispatchToVbucket = append(mock.calls.DispatchToVbucket, callInfo)
	mock.lockDispatchToVbucket.Unlock()
	return mock.DispatchToVbucketFunc(vbID)
}

// DispatchToVbucketCalls gets all the calls that were made to DispatchToVbucket.
// Check the length with:
//
//	len(mockedVbucketRouter.DispatchToVbucketCalls())
func (mock *VbucketRouterMock) DispatchToVbucketCalls() []struct {
	VbID uint16
} {
	var calls []struct {
		VbID uint16
	}
	mock.lockDispatchToVbucket.RLock()
	calls = mock.calls.DispatchToVbucket
	mock.lockDispatchToVbucket.RUnlock()
	return calls
}

// NumReplicas calls NumReplicasFunc.
func (mock *VbucketRouterMock) NumReplicas() (int, error) {
	if mock.NumReplicasFunc == nil {
		panic("VbucketRouterMock.NumReplicasFunc: method is nil but VbucketRouter.NumReplicas was just called")
	}
	callInfo := struct {
	}{}
	mock.lockNumReplicas.Lock()
	mock.calls.NumReplicas = append(mock.calls.NumReplicas, callInfo)
	mock.lockNumReplicas.Unlock()
	return mock.NumReplicasFunc()
}

// NumReplicasCalls gets all the calls that were made to NumReplicas.
// Check the length with:
//
//	len(mockedVbucketRouter.NumReplicasCalls())
func (mock *VbucketRouterMock) NumReplicasCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockNumReplicas.RLock()
	calls = mock.calls.NumReplicas
	mock.lockNumReplicas.RUnlock()
	return calls
}

// UpdateRoutingInfo calls UpdateRoutingInfoFunc.
func (mock *VbucketRouterMock) UpdateRoutingInfo(vbucketRoutingInfo *VbucketRoutingInfo) {
	if mock.UpdateRoutingInfoFunc == nil {
		panic("VbucketRouterMock.UpdateRoutingInfoFunc: method is nil but VbucketRouter.UpdateRoutingInfo was just called")
	}
	callInfo := struct {
		VbucketRoutingInfo *VbucketRoutingInfo
	}{
		VbucketRoutingInfo: vbucketRoutingInfo,
	}
	mock.lockUpdateRoutingInfo.Lock()
	mock.calls.UpdateRoutingInfo = append(mock.calls.UpdateRoutingInfo, callInfo)
	mock.lockUpdateRoutingInfo.Unlock()
	mock.UpdateRoutingInfoFunc(vbucketRoutingInfo)
}

// UpdateRoutingInfoCalls gets all the calls that were made to UpdateRoutingInfo.
// Check the length with:
//
//	len(mockedVbucketRouter.UpdateRoutingInfoCalls())
func (mock *VbucketRouterMock) UpdateRoutingInfoCalls() []struct {
	VbucketRoutingInfo *VbucketRoutingInfo
} {
	var calls []struct {
		VbucketRoutingInfo *VbucketRoutingInfo
	}
	mock.lockUpdateRoutingInfo.RLock()
	calls = mock.calls.UpdateRoutingInfo
	mock.lockUpdateRoutingInfo.RUnlock()
	return calls
}
