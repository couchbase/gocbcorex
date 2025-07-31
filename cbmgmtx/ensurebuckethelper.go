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

	BucketName   string
	BucketUUID   string
	WantMissing  bool
	WantSettings *MutableBucketSettings

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
	}.GetBucket(ctx, &GetBucketOptions{
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
		} else if errors.Is(err, ErrUnexpectedServerError) {
			// BUG(ING-1242): This is a workaround for a server bug.
			// The server sometimes responds with an unexpected server error when
			// trying to fetch a bucket that is in the process of being changed.
			e.Logger.Debug("target responded with an unexpected server error")
			return false, nil
		}

		e.Logger.Debug("target responded with an unexpected error", zap.Error(err))
		return false, err
	}

	if e.WantMissing {
		e.Logger.Debug("target responded successfully but we wanted a missing bucket")
		return false, nil
	}

	if e.BucketUUID != "" && resp.UUID != e.BucketUUID {
		e.Logger.Debug("target responded with success, but the bucket uuid did not match")
		if !e.WantMissing {
			return false, ErrBucketUuidMismatch
		} else {
			return true, nil
		}
	}

	if e.WantSettings != nil {
		if resp.FlushEnabled != e.WantSettings.FlushEnabled {
			e.Logger.Debug("target responded with success, but the flushEnabled setting did not match")
			return false, nil
		}
		if e.WantSettings.RAMQuotaMB > 0 && resp.RAMQuotaMB != e.WantSettings.RAMQuotaMB {
			e.Logger.Debug("target responded with success, but the RAMQuotaMB setting did not match")
			return false, nil
		}
		if e.WantSettings.EvictionPolicy != "" && resp.EvictionPolicy != e.WantSettings.EvictionPolicy {
			e.Logger.Debug("target responded with success, but the evictionPolicy setting did not match")
			return false, nil
		}
		if e.WantSettings.MaxTTL > 0 && resp.MaxTTL != e.WantSettings.MaxTTL {
			e.Logger.Debug("target responded with success, but the maxTTL setting did not match")
			return false, nil
		}
		if e.WantSettings.CompressionMode != "" && resp.CompressionMode != e.WantSettings.CompressionMode {
			e.Logger.Debug("target responded with success, but the compressionMode setting did not match")
			return false, nil
		}
		if e.WantSettings.DurabilityMinLevel != DurabilityLevelUnset && resp.DurabilityMinLevel != e.WantSettings.DurabilityMinLevel {
			e.Logger.Debug("target responded with success, but the durabilityMinLevel setting did not match")
			return false, nil
		}
		if e.WantSettings.HistoryRetentionBytes > 0 && resp.HistoryRetentionBytes != e.WantSettings.HistoryRetentionBytes {
			e.Logger.Debug("target responded with success, but the historyRetentionBytes setting did not match")
			return false, nil
		}
		if e.WantSettings.HistoryRetentionSeconds > 0 && resp.HistoryRetentionSeconds != e.WantSettings.HistoryRetentionSeconds {
			e.Logger.Debug("target responded with success, but the historyRetentionSeconds setting did not match")
			return false, nil
		}
		if e.WantSettings.HistoryRetentionCollectionDefault != nil {
			historyRetentionCollectionDefault := false
			if resp.HistoryRetentionCollectionDefault != nil {
				historyRetentionCollectionDefault = *resp.HistoryRetentionCollectionDefault
			}
			if historyRetentionCollectionDefault != *e.WantSettings.HistoryRetentionCollectionDefault {
				e.Logger.Debug("target responded with success, but the historyRetentionCollectionDefault setting did not match")
				return false, nil
			}
		}
	}

	e.Logger.Debug("target responded successfully")

	return true, nil
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
