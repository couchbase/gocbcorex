package core

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/couchbase/stellar-nebula/core/memdx"
)

var (
	ErrWaitingForVbucketMap = contextualDeadline{"still waiting for a vbucket map"}
)

type VbucketRouter interface {
	DispatchByKey(ctx context.Context, key []byte) (string, uint16, error)
	DispatchToVbucket(ctx context.Context, vbID uint16) (string, error)
}

type vbucketRoutingInfo struct {
	vbmap      *vbucketMap
	serverList []string
}

type vbucketRouter struct {
	routingInfo AtomicPointer[vbucketRoutingInfo]
	lock        sync.Mutex
	readyCh     chan struct{}
}

func newVbucketRouter() *vbucketRouter {
	vbd := &vbucketRouter{
		readyCh: make(chan struct{}, 1),
	}

	return vbd
}

func (vbd *vbucketRouter) UpdateRoutingInfo(info *vbucketRoutingInfo) {
	vbd.lock.Lock()
	defer vbd.lock.Unlock()

	vbd.routingInfo.Store(info)

	if vbd.readyCh != nil {
		close(vbd.readyCh)
		vbd.readyCh = nil
	}
}

func (vbd *vbucketRouter) getRoutingInfo(ctx context.Context) (*vbucketRoutingInfo, error) {
	routing := vbd.routingInfo.Load()
	if routing != nil {
		return routing, nil
	}

	return vbd.getRoutingInfoSlow(ctx)
}

func (vbd *vbucketRouter) getRoutingInfoSlow(ctx context.Context) (*vbucketRoutingInfo, error) {
	vbd.lock.Lock()

	routing := vbd.routingInfo.Load()
	if routing != nil {
		vbd.lock.Unlock()
		return routing, nil
	}

	readyCh := vbd.readyCh

	vbd.lock.Unlock()

	select {
	case <-readyCh:
	case <-ctx.Done():
		ctxErr := ctx.Err()
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return nil, ErrWaitingForVbucketMap
		} else {
			return nil, ctxErr
		}
	}

	return vbd.getRoutingInfoSlow(ctx)
}

func (vbd *vbucketRouter) DispatchByKey(ctx context.Context, key []byte) (string, uint16, error) {
	info, err := vbd.getRoutingInfo(ctx)
	if err != nil {
		return "", 0, err
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

func (vbd *vbucketRouter) DispatchToVbucket(ctx context.Context, vbID uint16) (string, error) {
	info, err := vbd.getRoutingInfo(ctx)
	if err != nil {
		return "", err
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

func OrchestrateMemdRouting[RespT any](
	ctx context.Context,
	vb VbucketRouter,
	cm ConfigManager,
	key []byte,
	fn func(endpoint string, vbID uint16) (RespT, error),
) (RespT, error) {
	for {
		endpoint, vbID, err := vb.DispatchByKey(ctx, key)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		// Implement me properly
		res, err := fn(endpoint, vbID)
		if err != nil {
			if errors.Is(err, memdx.ErrNotMyVbucket) {
				nmvberr := err.(memdx.NotMyVbucketError)
				cfg, parseErr := parseConfig(nmvberr.ConfigValue, endpoint)
				if parseErr == nil {
					cm.ApplyConfig(cfg.SourceHostname, cfg.Config)
					continue
				}
				log.Printf("Failed to parse not my vbucket response: %s", parseErr)
			}

			return res, err
		}

		return res, nil
	}
}
