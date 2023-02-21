package gocbcorex

import (
	"context"
	"errors"

	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

var (
	ErrNoVbucketMap = contextualDeadline{"no vbucket map is available"}
)

type VbucketRouter interface {
	UpdateRoutingInfo(*VbucketRoutingInfo)
	DispatchByKey(key []byte, replicaID uint32) (string, uint16, error)
	DispatchToVbucket(vbID uint16) (string, error)
}

type VbucketRoutingInfo struct {
	VbMap      *VbucketMap
	ServerList []string
}

type VbucketRouterOptions struct {
	Logger *zap.Logger
}

type vbucketRouter struct {
	logger      *zap.Logger
	routingInfo AtomicPointer[VbucketRoutingInfo]
}

var _ VbucketRouter = (*vbucketRouter)(nil)

func NewVbucketRouter(opts *VbucketRouterOptions) *vbucketRouter {
	if opts == nil {
		opts = &VbucketRouterOptions{}
	}

	vbd := &vbucketRouter{
		logger: loggerOrNop(opts.Logger),
	}

	return vbd
}

func (vbd *vbucketRouter) UpdateRoutingInfo(info *VbucketRoutingInfo) {
	vbd.routingInfo.Store(info)
}

func (vbd *vbucketRouter) getRoutingInfo() (*VbucketRoutingInfo, error) {
	routing := vbd.routingInfo.Load()
	if routing == nil {
		return nil, ErrNoVbucketMap
	}

	return routing, nil
}

func (vbd *vbucketRouter) DispatchByKey(key []byte, replicaIdx uint32) (string, uint16, error) {
	info, err := vbd.getRoutingInfo()
	if err != nil {
		return "", 0, err
	}

	vbID := info.VbMap.VbucketByKey(key)
	idx, err := info.VbMap.NodeByVbucket(vbID, replicaIdx)
	if err != nil {
		return "", 0, err
	}

	if idx < 0 || idx >= len(info.ServerList) {
		return "", 0, noServerAssignedError{
			RequestedVbId: vbID,
		}
	}

	return info.ServerList[idx], vbID, nil
}

func (vbd *vbucketRouter) DispatchToVbucket(vbID uint16) (string, error) {
	info, err := vbd.getRoutingInfo()
	if err != nil {
		return "", err
	}

	idx, err := info.VbMap.NodeByVbucket(vbID, 0)
	if err != nil {
		return "", err
	}

	if idx < 0 || idx >= len(info.ServerList) {
		return "", noServerAssignedError{
			RequestedVbId: vbID,
		}
	}

	return info.ServerList[idx], nil
}

func OrchestrateMemdRouting[RespT any](ctx context.Context, vb VbucketRouter, cm ConfigManager, key []byte, replicaIdx uint32,
	fn func(endpoint string, vbID uint16) (RespT, error)) (RespT, error) {
	endpoint, vbID, err := vb.DispatchByKey(key, replicaIdx)
	if err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	for {
		// Implement me properly
		res, err := fn(endpoint, vbID)
		if err != nil {
			if errors.Is(err, memdx.ErrNotMyVbucket) {
				var nmvErr memdx.ServerErrorWithConfig
				if !errors.As(err, &nmvErr) {
					// if there is no new config available, we cant make any assumptions
					// about the meaning of this error and propagate it upwards.
					// log.Printf("received a not-my-vbucket without config information")
					return res, err
				}

				cfg, parseErr := parseConfig(nmvErr.ConfigJson, endpoint)
				if parseErr != nil {
					// similar to above, if we can't parse the config, we cant make any
					// assumptions and need to propagate it.
					// log.Printf("failed to parse not my vbucket response: %s", parseErr)
					return res, err
				}

				cm.ApplyConfig(cfg.SourceHostname, cfg.Config)

				newEndpoint, newVbID, err := vb.DispatchByKey(key, replicaIdx)
				if err != nil {
					var emptyResp RespT
					return emptyResp, err
				}

				if newEndpoint == endpoint && newVbID == vbID {
					// if after the update we are going to be sending the request back
					// to the place that rejected it, we consider this non-deterministic
					// and fall back to the application to deal with (or retries).
					return res, err
				}

				endpoint = newEndpoint
				vbID = newVbID
				continue
			}

			return res, err
		}

		return res, nil
	}
}
