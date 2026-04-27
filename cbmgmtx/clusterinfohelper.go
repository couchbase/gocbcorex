package cbmgmtx

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
)

type ClusterInfoHelper struct {
	Logger     *zap.Logger
	UserAgent  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

type ClusterInfoNodeResult struct {
	Endpoint     string
	IsEnterprise bool
}

type AggregatedClusterInfoResponse struct {
	Uuid          string
	AllEnterprise bool
	AllCommunity  bool
	Nodes         []ClusterInfoNodeResult
}

type GetAggregatedClusterInfoOptions struct {
	Transport http.RoundTripper
	Targets   []NodeTarget
}

func (h *ClusterInfoHelper) fetchOne(
	ctx context.Context,
	httpRoundTripper http.RoundTripper,
	target NodeTarget,
) (*ClusterInfoResponse, error) {
	h.Logger.Debug("fetching cluster info from target",
		zap.String("endpoint", target.Endpoint),
		zap.String("username", target.Username))

	resp, err := Management{
		Transport: httpRoundTripper,
		UserAgent: h.UserAgent,
		Endpoint:  target.Endpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: target.Username,
			Password: target.Password,
		},
	}.GetClusterInfo(ctx, &GetClusterInfoOptions{
		OnBehalfOf: h.OnBehalfOf,
	})

	if err != nil {
		h.Logger.Debug("target responded with an error", zap.Error(err))
		return nil, err
	}

	h.Logger.Debug("target responded successfully",
		zap.String("endpoint", target.Endpoint),
		zap.Bool("isEnterprise", resp.IsEnterprise))

	return resp, nil
}

func (h *ClusterInfoHelper) FetchAll(
	ctx context.Context,
	opts *GetAggregatedClusterInfoOptions,
) (*AggregatedClusterInfoResponse, error) {
	if len(opts.Targets) == 0 {
		return nil, errors.New("no targets available to fetch cluster info")
	}

	var clusterUuid string
	allEnterprise := true
	allCommunity := true
	nodes := make([]ClusterInfoNodeResult, 0, len(opts.Targets))

	for _, target := range opts.Targets {
		resp, err := h.fetchOne(ctx, opts.Transport, target)
		if err != nil {
			return nil, err
		}

		if clusterUuid == "" {
			clusterUuid = resp.Uuid
		} else if resp.Uuid != clusterUuid {
			return nil, fmt.Errorf("cluster uuid mismatch: expected %s, got %s from %s",
				clusterUuid, resp.Uuid, target.Endpoint)
		}

		if resp.IsEnterprise {
			allCommunity = false
		} else {
			allEnterprise = false
		}

		nodes = append(nodes, ClusterInfoNodeResult{
			Endpoint:     target.Endpoint,
			IsEnterprise: resp.IsEnterprise,
		})
	}

	return &AggregatedClusterInfoResponse{
		Uuid:          clusterUuid,
		AllEnterprise: allEnterprise,
		AllCommunity:  allCommunity,
		Nodes:         nodes,
	}, nil
}
