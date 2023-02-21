package gocbcorex

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// QueryScanConsistency indicates the level of data consistency desired for a query.
type QueryScanConsistency uint

const (
	// QueryScanConsistencyNotBounded indicates no data consistency is required.
	QueryScanConsistencyNotBounded QueryScanConsistency = iota + 1
	// QueryScanConsistencyRequestPlus indicates that request-level data consistency is required.
	QueryScanConsistencyRequestPlus
)

// QueryProfileMode specifies the profiling mode to use during a query.
type QueryProfileMode string

const (
	// QueryProfileModeNone disables query profiling
	QueryProfileModeNone QueryProfileMode = "off"

	// QueryProfileModePhases includes phase profiling information in the query response
	QueryProfileModePhases QueryProfileMode = "phases"

	// QueryProfileModeTimings includes timing profiling information in the query response
	QueryProfileModeTimings QueryProfileMode = "timings"
)

// QueryOptions represents the options available when executing a query.
type QueryOptions struct {
	ScanConsistency QueryScanConsistency
	// ConsistentWith  *MutationState TODO(chvck): MutationState
	Profile QueryProfileMode

	// ScanCap is the maximum buffered channel size between the indexer connectionManager and the query service for index scans.
	ScanCap uint32

	// PipelineBatch controls the number of items execution operators can batch for Fetch from the KV.
	PipelineBatch uint32

	// PipelineCap controls the maximum number of items each execution operator can buffer between various operators.
	PipelineCap uint32

	// ScanWait is how long the indexer is allowed to wait until it can satisfy ScanConsistency/ConsistentWith criteria.
	ScanWait time.Duration
	Readonly bool

	// MaxParallelism is the maximum number of index partitions, for computing aggregation in parallel.
	MaxParallelism uint32

	// ClientContextID provides a unique ID for this query which can be used matching up requests between connectionManager and
	// server. If not provided will be assigned a uuid value.
	ClientContextID      string
	PositionalParameters []interface{}
	NamedParameters      map[string]interface{}
	Metrics              bool

	// Raw provides a way to provide extra parameters in the request body for the query.
	Raw map[string]interface{}

	// FlexIndex tells the query engine to use a flex index (utilizing the search service).
	FlexIndex bool

	// PreserveExpiry tells the query engine to preserve expiration values set on any documents modified by this query.
	PreserveExpiry bool

	Statement string

	// Endpoint overrides internal routing of requests to send a request directly to an endpoint.
	Endpoint string
}

func (opts *QueryOptions) toMap(ctx context.Context) (map[string]interface{}, error) {
	execOpts := make(map[string]interface{})

	// if opts.ScanConsistency != 0 && opts.ConsistentWith != nil {
	// 	return nil, placeholderError{"ScanConsistency and ConsistentWith must be used exclusively"}
	// }	TODO(chvck): Needs MutationState

	if opts.ScanConsistency != 0 {
		if opts.ScanConsistency == QueryScanConsistencyNotBounded {
			execOpts["scan_consistency"] = "not_bounded"
		} else if opts.ScanConsistency == QueryScanConsistencyRequestPlus {
			execOpts["scan_consistency"] = "request_plus"
		} else {
			return nil, placeholderError{"Unexpected consistency option"}
		}
	}

	// if opts.ConsistentWith != nil {
	// 	execOpts["scan_consistency"] = "at_plus"
	// 	execOpts["scan_vectors"] = opts.ConsistentWith
	// }	TODO(chvck): Needs MutationState

	if opts.Profile != "" {
		execOpts["profile"] = opts.Profile
	}

	if opts.Readonly {
		execOpts["readonly"] = opts.Readonly
	}

	if opts.PositionalParameters != nil && opts.NamedParameters != nil {
		return nil, placeholderError{"Positional and named parameters must be used exclusively"}
	}

	if opts.PositionalParameters != nil {
		execOpts["args"] = opts.PositionalParameters
	}

	if opts.NamedParameters != nil {
		for key, value := range opts.NamedParameters {
			if !strings.HasPrefix(key, "$") {
				key = "$" + key
			}
			execOpts[key] = value
		}
	}

	if opts.ScanCap != 0 {
		execOpts["scan_cap"] = strconv.FormatUint(uint64(opts.ScanCap), 10)
	}

	if opts.PipelineBatch != 0 {
		execOpts["pipeline_batch"] = strconv.FormatUint(uint64(opts.PipelineBatch), 10)
	}

	if opts.PipelineCap != 0 {
		execOpts["pipeline_cap"] = strconv.FormatUint(uint64(opts.PipelineCap), 10)
	}

	if opts.ScanWait > 0 {
		execOpts["scan_wait"] = opts.ScanWait.String()
	}

	if opts.Raw != nil {
		for k, v := range opts.Raw {
			execOpts[k] = v
		}
	}

	if opts.MaxParallelism > 0 {
		execOpts["max_parallelism"] = strconv.FormatUint(uint64(opts.MaxParallelism), 10)
	}

	if !opts.Metrics {
		execOpts["metrics"] = false
	}

	if opts.MaxParallelism > 0 {
		execOpts["max_parallelism"] = strconv.FormatUint(uint64(opts.MaxParallelism), 10)
	}

	if !opts.Metrics {
		execOpts["metrics"] = false
	}

	if opts.ClientContextID == "" {
		execOpts["client_context_id"] = uuid.New()
	} else {
		execOpts["client_context_id"] = opts.ClientContextID
	}

	if opts.FlexIndex {
		execOpts["use_fts"] = true
	}

	if opts.PreserveExpiry {
		execOpts["preserve_expiry"] = true
	}

	if deadline, ok := ctx.Deadline(); ok {
		execOpts["timeout"] = time.Until(deadline).String()
	}

	execOpts["statement"] = opts.Statement

	return execOpts, nil
}
