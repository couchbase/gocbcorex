package memdx

import (
	"errors"
)

type OpBootstrapEncoder interface {
	Hello(Dispatcher, *HelloRequest, func(*HelloResponse, error)) (PendingOp, error)
	GetErrorMap(Dispatcher, *GetErrorMapRequest, func([]byte, error)) (PendingOp, error)
	OpSaslAuthAutoEncoder
	SelectBucket(Dispatcher, *SelectBucketRequest, func(error)) (PendingOp, error)
	GetClusterConfig(Dispatcher, *GetClusterConfigRequest, func([]byte, error)) (PendingOp, error)
}

var _ OpBootstrapEncoder = (*OpsCore)(nil)

// OpBootstrap implements automatic pipelining of the 5 standard bootstrap
// operations that a client needs to perform to set up a connection.
type OpBootstrap struct {
	Encoder OpBootstrapEncoder
}

type BootstrapOptions struct {
	Hello            *HelloRequest
	GetErrorMap      *GetErrorMapRequest
	Auth             *SaslAuthAutoOptions
	SelectBucket     *SelectBucketRequest
	GetClusterConfig *GetClusterConfigRequest
}

type BootstrapResult struct {
	Hello         *HelloResponse
	ErrorMap      []byte
	ClusterConfig []byte
}

func (a OpBootstrap) Bootstrap(d Dispatcher, opts *BootstrapOptions, cb func(res *BootstrapResult, err error)) (PendingOp, error) {
	// NOTE(brett19): The following logic is dependant on operation ordering that
	// is guarenteed by memcached, even when Out-Of-Order Execution is enabled.

	result := &BootstrapResult{}
	pipeline := OpPipeline{}

	if opts.Hello != nil {
		OpPipelineAdd(&pipeline, func(opCb func(res *HelloResponse, err error)) (PendingOp, error) {
			return a.Encoder.Hello(d, opts.Hello, opCb)
		}, func(res *HelloResponse, err error) bool {
			if err != nil {
				// when an error occurs, we dont fail bootstrap entirely, we instead
				// return the result indicating no Hello result...
				res = nil
			}

			result.Hello = res
			return true
		})
	}

	if opts.GetErrorMap != nil {
		OpPipelineAdd(&pipeline, func(opCb func(errorMap []byte, err error)) (PendingOp, error) {
			return a.Encoder.GetErrorMap(d, opts.GetErrorMap, opCb)
		}, func(errorMap []byte, err error) bool {
			if err != nil {
				// when an error occurs, we dont fail bootstrap entirely, we instead
				// return the result indicating no ErrorMap result...
				errorMap = nil
			}

			result.ErrorMap = errorMap
			return true
		})
	}

	if opts.Auth != nil {
		OpPipelineAddWithNext(&pipeline, func(nextFn func(), opCb func(res struct{}, err error)) (PendingOp, error) {
			return OpSaslAuthAuto{
				Encoder: a.Encoder,
			}.SASLAuthAuto(d, opts.Auth, nextFn, func(err error) {
				opCb(struct{}{}, err)
			})
		}, func(res struct{}, err error) bool {
			if err != nil {
				cb(nil, err)
				return false
			}

			return true
		})
	}

	if opts.SelectBucket != nil {
		OpPipelineAdd(&pipeline, func(opCb func(res struct{}, err error)) (PendingOp, error) {
			return a.Encoder.SelectBucket(d, opts.SelectBucket, func(err error) {
				opCb(struct{}{}, err)
			})
		}, func(res struct{}, err error) bool {
			if err != nil {
				cb(nil, err)
				return false
			}

			return true
		})
	}

	if opts.GetClusterConfig != nil {
		OpPipelineAdd(&pipeline, func(opCb func(clusterConfig []byte, err error)) (PendingOp, error) {
			return a.Encoder.GetClusterConfig(d, opts.GetClusterConfig, opCb)
		}, func(clusterConfig []byte, err error) bool {
			if err != nil {
				// when an error occurs, we dont fail bootstrap entirely, we instead
				// return the result indicating no Config result...
				clusterConfig = nil
			}

			result.ClusterConfig = clusterConfig
			return true
		})
	}

	OpPipelineAddSync(&pipeline, func() {
		cb(result, nil)
	})

	return pipeline.Start(), nil
}

func (a OpBootstrap) isRequestCancelledError(err error) bool {
	var cancelErr requestCancelledError
	return errors.As(err, &cancelErr)
}
