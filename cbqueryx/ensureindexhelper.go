package cbqueryx

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type EnsureIndexHelper struct {
	Logger     *zap.Logger
	UserAgent  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo

	BucketName     string
	ScopeName      string
	CollectionName string
	IndexName      string

	confirmedEndpoints []string
}

type NodeTarget struct {
	Endpoint string
	Username string
	Password string
}

type EnsureIndexPollOptions struct {
	Transport http.RoundTripper
	Targets   []NodeTarget
}

func (e *EnsureIndexHelper) PollCreated(ctx context.Context, opts *EnsureIndexPollOptions) (bool, error) {
	return e.pollAll(ctx, opts, func(numRows int) bool {
		// If there are rows then the endpoint knows the index.
		if numRows == 0 {
			e.Logger.Debug("target responded with success, but the target still didn't have the index")
			return false
		} else if numRows > 1 {
			e.Logger.Debug("query returned %d rows rather than 1", zap.Int("num-rows", numRows))
		}

		return true
	})
}

func (e *EnsureIndexHelper) PollDropped(ctx context.Context, opts *EnsureIndexPollOptions) (bool, error) {
	return e.pollAll(ctx, opts, func(numRows int) bool {
		// If there are rows then the endpoint knows the index.
		if numRows > 0 {
			e.Logger.Debug("target responded with success, but the target still had the index")
			return false
		}

		return true
	})
}

func (e *EnsureIndexHelper) keyspace() string {
	isDefaultScope := e.ScopeName == "_default"
	isDefaultCollection := e.CollectionName == "_default"

	if isDefaultScope && isDefaultCollection {
		return "bucket_id IS MISSING AND keyspace_id=$bucket"
	} else if isDefaultScope {
		return "bucket_id=$bucket AND scope_id=_default AND keyspace_id=$collection"
	} else if isDefaultCollection {
		return "bucket_id=$bucket AND scope_id=$scope AND keyspace_id=_default"
	} else {
		return "bucket_id=$bucket AND scope_id=$scope AND keyspace_id=$collection"
	}
}

func (e *EnsureIndexHelper) pollOne(
	ctx context.Context,
	httpRoundTripper http.RoundTripper, target NodeTarget,
) (ResultStream, error) {
	e.Logger.Debug("polling a single target",
		zap.String("endpoint", target.Endpoint),
		zap.String("username", target.Username))

	statement := "SELECT `idx`.name FROM system:indexes AS idx WHERE " + e.keyspace() +
		" AND name=$name AND `using` = \"gsi\" "

	var encodingErr error
	encodeString := func(val string) json.RawMessage {
		res, err := json.Marshal(val)
		if err != nil {
			encodingErr = err
		}

		return res
	}

	args := map[string]json.RawMessage{
		"bucket":     encodeString(e.BucketName),
		"scope":      encodeString(e.ScopeName),
		"collection": encodeString(e.CollectionName),
		"name":       encodeString(e.IndexName),
	}
	if encodingErr != nil {
		return nil, encodingErr
	}

	resp, err := Query{
		Transport: httpRoundTripper,
		UserAgent: e.UserAgent,
		Endpoint:  target.Endpoint,
		Username:  target.Username,
		Password:  target.Password,
	}.Query(ctx, &Options{
		ReadOnly:   true,
		OnBehalfOf: e.OnBehalfOf,
		Statement:  statement,
		NamedArgs:  args,
	})

	if err != nil {
		e.Logger.Debug("target responded with an unexpected error", zap.Error(err))
		return nil, err
	}

	return resp, nil
}

func (e *EnsureIndexHelper) pollAll(ctx context.Context,
	opts *EnsureIndexPollOptions, cb func(int) bool) (bool, error) {
	filteredTargets := make([]NodeTarget, 0, len(opts.Targets))
	for _, target := range opts.Targets {
		if !slices.Contains(e.confirmedEndpoints, target.Endpoint) {
			filteredTargets = append(filteredTargets, target)
		}
	}

	var successEndpoints []string
	for _, target := range filteredTargets {
		resp, err := e.pollOne(ctx, opts.Transport, target)
		if err != nil {
			return false, err
		}

		var numRows int
		for resp.HasMoreRows() {
			_, err := resp.ReadRow()
			if err != nil {
				e.Logger.Debug("read row failed with an unexpected error", zap.Error(err))
				return false, err
			}

			numRows++
		}

		if cb(numRows) {
			e.Logger.Debug("target successfully checked")

			successEndpoints = append(successEndpoints, target.Endpoint)
		}
	}

	e.confirmedEndpoints = append(e.confirmedEndpoints, successEndpoints...)

	if len(successEndpoints) != len(filteredTargets) {
		// some of the endpoints still need to be successful
		return false, nil
	}

	return true, nil
}
