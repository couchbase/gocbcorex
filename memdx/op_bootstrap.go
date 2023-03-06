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

	const (
		stageHello         = 0
		stageErrorMap      = 1
		stageAuth          = 2
		stageSelectBucket  = 3
		stageClusterConfig = 4
		stageCallback      = 5
	)

	// we don't need to lock because we do everything with these objects ourselves
	// first, and then dispatch them all at once, relying on the fact that the callbacks
	// are always invoked by a single reader thread.  result holds the result data,
	// and currentStage represents the current stage that is being attempted.
	currentStage := stageHello
	result := &BootstrapResult{}

	var dispatchHello func() error
	var dispatchErrorMap func() error
	var dispatchAuth func() error
	var dispatchSelectBucket func() error
	var dispatchClusterConfig func() error
	var dispatchCallback func() error

	maybeCallback := func() {
		if currentStage == stageHello && opts.Hello == nil {
			currentStage = stageErrorMap
		}
		if currentStage == stageErrorMap && opts.GetErrorMap == nil {
			currentStage = stageAuth
		}
		if currentStage == stageAuth && opts.Auth == nil {
			currentStage = stageSelectBucket
		}
		if currentStage == stageSelectBucket && opts.SelectBucket == nil {
			currentStage = stageClusterConfig
		}
		if currentStage == stageClusterConfig && opts.GetClusterConfig == nil {
			currentStage = stageCallback
		}

		if currentStage == stageCallback {
			cb(result, nil)
		}
	}

	pendingOp := &multiPendingOp{}

	dispatchHello = func() error {
		if opts.Hello == nil {
			return dispatchErrorMap()
		}

		op, err := a.Encoder.Hello(d, opts.Hello, func(resp *HelloResponse, err error) {
			if currentStage != stageHello {
				return
			}

			if err != nil {
				if a.isRequestCancelledError(err) {
					cb(nil, err)
					return
				}
				// when an error occurs, we dont fail bootstrap entirely, we instead
				// return the result indicating no Hello result...
				resp = nil
			}

			result.Hello = resp
			currentStage = stageErrorMap
			maybeCallback()
		})
		if err != nil {
			return err
		}
		pendingOp.Add(op)

		return dispatchErrorMap()
	}

	dispatchErrorMap = func() error {
		if opts.GetErrorMap == nil {
			return dispatchAuth()
		}

		op, err := a.Encoder.GetErrorMap(d, opts.GetErrorMap, func(errorMap []byte, err error) {
			if currentStage != stageErrorMap {
				return
			}

			if err != nil {
				if a.isRequestCancelledError(err) {
					cb(nil, err)
					return
				}
				// when an error occurs, we dont fail bootstrap entirely, we instead
				// return the result indicating no ErrorMap result...
				errorMap = nil
			}

			result.ErrorMap = errorMap
			currentStage = stageAuth
			maybeCallback()
		})
		if err != nil {
			return err
		}
		pendingOp.Add(op)

		return dispatchAuth()
	}

	dispatchAuth = func() error {
		if opts.Auth == nil {
			return dispatchSelectBucket()
		}

		op, err := OpSaslAuthAuto{
			Encoder: a.Encoder,
		}.SASLAuthAuto(d, opts.Auth, func() {
			err := dispatchSelectBucket()
			if err != nil {
				cb(nil, err)
			}
		}, func(err error) {
			if currentStage != stageAuth {
				return
			}

			if err != nil {
				cb(nil, err)
				return
			}

			currentStage = stageSelectBucket
			maybeCallback()
		})
		if err != nil {
			return err
		}
		pendingOp.Add(op)

		return nil
	}

	dispatchSelectBucket = func() error {
		if opts.SelectBucket == nil {
			return dispatchClusterConfig()
		}

		op, err := a.Encoder.SelectBucket(d, opts.SelectBucket, func(err error) {
			if currentStage != stageSelectBucket {
				return
			}

			if err != nil {
				cb(nil, err)
				return
			}

			currentStage = stageClusterConfig
			maybeCallback()
		})
		if err != nil {
			return err
		}
		pendingOp.Add(op)

		return dispatchClusterConfig()
	}

	dispatchClusterConfig = func() error {
		if opts.GetClusterConfig == nil {
			return dispatchCallback()
		}

		op, err := a.Encoder.GetClusterConfig(d, opts.GetClusterConfig, func(clusterConfig []byte, err error) {
			if currentStage != stageClusterConfig {
				return
			}

			if err != nil {
				if a.isRequestCancelledError(err) {
					cb(nil, err)
					return
				}
				// when an error occurs, we dont fail bootstrap entirely, we instead
				// return the result indicating no Config result...
				clusterConfig = nil
			}

			result.ClusterConfig = clusterConfig
			currentStage = stageCallback
			maybeCallback()
		})
		if err != nil {
			return err
		}
		pendingOp.Add(op)

		return dispatchCallback()
	}

	dispatchCallback = func() error {
		// this function does nothing because we have no way to easily
		// schedule a callback to be invoked serialy by the connection
		// so we rely on maybeCallback() to handle this behaviour.
		return nil
	}

	// maybeCallback must be invoked before any dispatches to ensure
	// we still own all the state and to avoid racing those threads.
	maybeCallback()

	if err := dispatchHello(); err != nil {
		return nil, err
	}

	return pendingOp, nil
}

func (a OpBootstrap) isRequestCancelledError(err error) bool {
	var cancelErr requestCancelledError
	return errors.As(err, &cancelErr)
}
