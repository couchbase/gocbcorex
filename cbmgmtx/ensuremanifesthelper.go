package cbmgmtx

import (
	"context"
	"net/http"
	"strconv"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type EnsureManifestHelper struct {
	Logger     *zap.Logger
	UserAgent  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo

	BucketName    string
	CollectionUid uint64

	confirmedEndpoints []string
}

func (e *EnsureManifestHelper) pollOne(
	ctx context.Context,
	httpRoundTripper http.RoundTripper, target NodeTarget,
) (bool, error) {
	e.Logger.Debug("polling a single target",
		zap.String("endpoint", target.Endpoint),
		zap.String("username", target.Username))

	resp, err := Management{
		Transport: httpRoundTripper,
		UserAgent: e.UserAgent,
		Endpoint:  target.Endpoint,
		Username:  target.Username,
		Password:  target.Password,
	}.GetCollectionManifest(ctx, &GetCollectionManifestOptions{
		BucketName: e.BucketName,
		OnBehalfOf: e.OnBehalfOf,
	})

	if err != nil {
		e.Logger.Debug("target responded with an unexpected error", zap.Error(err))
		return false, err
	}

	collectionUid, _ := strconv.ParseUint(resp.UID, 16, 64)
	if collectionUid < e.CollectionUid {
		e.Logger.Debug("target responded with success, but the manifest is not up to date",
			zap.Uint64("identified", collectionUid),
			zap.Uint64("wanted", e.CollectionUid))
		return false, nil
	}

	e.Logger.Debug("target successfully checked",
		zap.Uint64("identified", collectionUid),
		zap.Uint64("wanted", e.CollectionUid))

	return true, nil
}

type EnsureManifestPollOptions struct {
	Transport http.RoundTripper
	Targets   []NodeTarget
}

func (e *EnsureManifestHelper) Poll(ctx context.Context, opts *EnsureManifestPollOptions) (bool, error) {
	filteredTargets := make([]NodeTarget, 0, len(opts.Targets))
	for _, target := range opts.Targets {
		if !slices.Contains(e.confirmedEndpoints, target.Endpoint) {
			filteredTargets = append(filteredTargets, target)
		}
	}

	var successEndpoints []string
	for _, target := range filteredTargets {
		res, err := e.pollOne(ctx, opts.Transport, target)
		if err != nil {
			return false, err
		}

		if !res {
			continue
		}

		successEndpoints = append(successEndpoints, target.Endpoint)
	}

	e.confirmedEndpoints = append(e.confirmedEndpoints, successEndpoints...)

	if len(successEndpoints) != len(filteredTargets) {
		// some of the endpoints still need to be successful
		return false, nil
	}

	return true, nil
}
