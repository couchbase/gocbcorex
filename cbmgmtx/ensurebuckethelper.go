package cbmgmtx

import (
	"context"
	"errors"
	"net/http"

	"github.com/couchbase/gocbcorex/zaputils"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type EnsureBucketHelper struct {
	Logger     *zap.Logger
	UserAgent  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo

	BucketName  string
	BucketUUID  string
	WantMissing bool

	confirmedEndpoints []string
}

type NodeTarget struct {
	Endpoint string
	Username string
	Password string
}

func (e *EnsureBucketHelper) pollOne(
	ctx context.Context,
	httpRoundTripper http.RoundTripper,
	target NodeTarget,
) (bool, error) {
	e.Logger.Debug("polling a single target",
		zap.String("endpoint", target.Endpoint),
		zap.String("username", target.Username),
		zaputils.BucketName("bucketName", e.BucketName),
		zap.String("bucketUuid", e.BucketUUID),
		zap.Bool("wantMissing", e.WantMissing))

	resp, err := Management{
		Transport: httpRoundTripper,
		UserAgent: e.UserAgent,
		Endpoint:  target.Endpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: target.Username,
			Password: target.Password,
		},
	}.GetTerseBucketConfig(ctx, &GetTerseBucketConfigOptions{
		BucketName: e.BucketName,
		OnBehalfOf: e.OnBehalfOf,
	})

	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			e.Logger.Debug("target responded with bucket not found")
			if !e.WantMissing {
				return false, nil
			} else {
				return true, nil
			}
		}

		e.Logger.Debug("target responded with an unexpected error", zap.Error(err))
		return false, err
	}

	if e.BucketUUID != "" && resp.UUID != e.BucketUUID {
		e.Logger.Debug("target responded with success, but the bucket uuid did not match")
		if !e.WantMissing {
			return false, ErrBucketUuidMismatch
		} else {
			return true, nil
		}
	}

	e.Logger.Debug("target responded successfully")

	if !e.WantMissing {
		return true, nil
	} else {
		return false, nil
	}
}

type EnsureBucketPollOptions struct {
	Transport http.RoundTripper
	Targets   []NodeTarget
}

func (e *EnsureBucketHelper) Poll(ctx context.Context, opts *EnsureBucketPollOptions) (bool, error) {
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
