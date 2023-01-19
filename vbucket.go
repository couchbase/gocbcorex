package core

import "sync/atomic"

type DispatchByKeyHandler func(string, uint16, error)
type DispatchToVbucketHandler func(string, error)

type VbucketDispatcher interface {
	DispatchByKey(ctx *AsyncContext, key []byte, cb DispatchByKeyHandler) error
	DispatchToVbucket(ctx *AsyncContext, vbID uint16, cb DispatchToVbucketHandler) error
	StoreVbucketRoutingInfo(info *vbucketRoutingInfo)
	Close() error
}

type vbucketRoutingInfo struct {
	vbmap      *vbucketMap
	serverList []string
}

type vbucketDispatcher struct {
	routingInfo  AtomicPointer[vbucketRoutingInfo]
	pendingQueue *queue[*comparableQueueItem[func()]]
	readyCh      chan struct{}

	ready uint32
}

func newVbucketDispatcher() *vbucketDispatcher {
	vbd := &vbucketDispatcher{
		pendingQueue: newQueue[*comparableQueueItem[func()]](2048),
		readyCh:      make(chan struct{}, 1),
	}
	go vbd.process()

	return vbd
}

func (vbd *vbucketDispatcher) process() {
	<-vbd.readyCh
	for {
		next := vbd.pendingQueue.Next()
		if next == nil {
			return
		}

		(*next).value()
	}
}

func (vbd *vbucketDispatcher) Close() error {
	vbd.pendingQueue.Close()
	if atomic.CompareAndSwapUint32(&vbd.ready, 0, 1) {
		close(vbd.readyCh)
	}

	return nil
}

func (vbd *vbucketDispatcher) StoreVbucketRoutingInfo(info *vbucketRoutingInfo) {
	if atomic.CompareAndSwapUint32(&vbd.ready, 0, 1) {
		close(vbd.readyCh)
	}

	vbd.storeRoutingInfo(info)
}

func (vbd *vbucketDispatcher) DispatchByKey(ctx *AsyncContext, key []byte, cb DispatchByKeyHandler) error {
	handler := func() {
		info := vbd.loadRoutingInfo()
		if info == nil {
			cb("", 0, placeholderError{"imnotgood"})
			return
		}

		vbID := info.vbmap.VbucketByKey(key)
		idx, err := info.vbmap.NodeByVbucket(vbID, 0)
		if err != nil {
			cb("", 0, err)
			return
		}

		// TODO: This really shouldn't be possible, and should possibly also be a panic condition?
		if idx > len(info.serverList) {
			cb("", 0, placeholderError{"imnotgood"})
			return
		}

		cb(info.serverList[idx], vbID, nil)
	}

	return vbd.pendingQueue.Push(&comparableQueueItem[func()]{handler})

}

func (vbd *vbucketDispatcher) DispatchToVbucket(ctx *AsyncContext, vbID uint16, cb DispatchToVbucketHandler) error {
	handler := func() {
		info := vbd.loadRoutingInfo()
		if info == nil {
			cb("", placeholderError{"imnotgood"})
			return
		}

		idx, err := info.vbmap.NodeByVbucket(vbID, 0)
		if err != nil {
			cb("", err)
			return
		}

		// TODO: This really shouldn't be possible, and should possibly also be a panic condition?
		if idx > len(info.serverList) {
			cb("", placeholderError{"imnotgood"})
			return
		}

		cb(info.serverList[idx], nil)
	}

	return vbd.pendingQueue.Push(&comparableQueueItem[func()]{handler})
}

func (vbd *vbucketDispatcher) loadRoutingInfo() *vbucketRoutingInfo {
	return vbd.routingInfo.Load()
}

func (vbd *vbucketDispatcher) storeRoutingInfo(new *vbucketRoutingInfo) {
	vbd.routingInfo.Store(new)
}
