package core

import (
	"context"
	"errors"
	"sync/atomic"
)

var (
	ErrWaitingForVbucketMap = contextualDeadline{"still waiting for a vbucket map"}
)

type VbucketDispatcher interface {
	DispatchByKey(ctx context.Context, key []byte) (string, uint16, error)
	DispatchToVbucket(ctx context.Context, vbID uint16) (string, error)
	StoreVbucketRoutingInfo(info *vbucketRoutingInfo)
	Close() error
}

type vbucketRoutingInfo struct {
	vbmap      *vbucketMap
	serverList []string
}

type vbucketDispatcher struct {
	routingInfo AtomicPointer[vbucketRoutingInfo]

	ready    uint32
	readyCh  chan struct{}
	closedCh chan struct{}
}

func newVbucketDispatcher() *vbucketDispatcher {
	vbd := &vbucketDispatcher{
		readyCh:  make(chan struct{}, 1),
		closedCh: make(chan struct{}, 1),
	}

	return vbd
}

func (vbd *vbucketDispatcher) Close() error {
	vbd.StoreVbucketRoutingInfo(nil)
	close(vbd.closedCh)

	return nil
}

func (vbd *vbucketDispatcher) StoreVbucketRoutingInfo(info *vbucketRoutingInfo) {
	// TODO: gross
	if info != nil && atomic.CompareAndSwapUint32(&vbd.ready, 0, 1) {
		close(vbd.readyCh)
	}

	vbd.routingInfo.Store(info)
}

func (vbd *vbucketDispatcher) waitToBeReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		ctxErr := ctx.Err()
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return ErrWaitingForVbucketMap
		} else {
			return ctxErr
		}
	case <-vbd.readyCh:
		return nil
	case <-vbd.closedCh:
		return nil
	}
}

func (vbd *vbucketDispatcher) DispatchByKey(ctx context.Context, key []byte) (string, uint16, error) {
	if err := vbd.waitToBeReady(ctx); err != nil {
		return "", 0, err
	}

	info := vbd.loadRoutingInfo()
	if info == nil {
		return "", 0, placeholderError{"imnotgood"}
	}

	vbID := info.vbmap.VbucketByKey(key)
	idx, err := info.vbmap.NodeByVbucket(vbID, 0)
	if err != nil {
		return "", 0, err
	}

	// TODO: This really shouldn't be possible, and should possibly also be a panic condition?
	if idx > len(info.serverList) {
		return "", 0, placeholderError{"imnotgood"}
	}

	return info.serverList[idx], vbID, nil
}

func (vbd *vbucketDispatcher) DispatchToVbucket(ctx context.Context, vbID uint16) (string, error) {
	if err := vbd.waitToBeReady(ctx); err != nil {
		return "", err
	}

	info := vbd.loadRoutingInfo()
	if info == nil {
		return "", placeholderError{"imnotgood"}
	}

	idx, err := info.vbmap.NodeByVbucket(vbID, 0)
	if err != nil {
		return "", err
	}

	// TODO: This really shouldn't be possible, and should possibly also be a panic condition?
	if idx > len(info.serverList) {
		return "", placeholderError{"imnotgood"}
	}

	return info.serverList[idx], nil
}

func (vbd *vbucketDispatcher) loadRoutingInfo() *vbucketRoutingInfo {
	return vbd.routingInfo.Load()
}
