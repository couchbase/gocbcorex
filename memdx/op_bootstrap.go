package memdx

import "log"

type OpBootstrapEncoder interface {
	Hello(Dispatcher, *HelloRequest, func(*HelloResponse, error)) error
	GetErrorMap(Dispatcher, *GetErrorMapRequest, func([]byte, error)) error
	OpSaslAuthAutoEncoder
	SelectBucket(Dispatcher, *SelectBucketRequest, func(error)) error
	GetClusterConfig(Dispatcher, *GetClusterConfigRequest, func([]byte, error)) error
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

func (a OpBootstrap) Execute(d Dispatcher, opts *BootstrapOptions, cb func(res *BootstrapResult, err error)) {
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

	var dispatchHello func()
	var dispatchErrorMap func()
	var dispatchAuth func()
	var dispatchSelectBucket func()
	var dispatchClusterConfig func()
	var dispatchCallback func()

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

	dispatchHello = func() {
		if opts.Hello == nil {
			dispatchErrorMap()
			return
		}

		log.Printf("Hello sending...")
		a.Encoder.Hello(d, opts.Hello, func(resp *HelloResponse, err error) {
			if currentStage != stageHello {
				return
			}

			if err != nil {
				// when an error occurs, we dont fail bootstrap entirely, we instead
				// return the result indicating no Hello result...
				resp = nil
			}

			result.Hello = resp
			currentStage = stageErrorMap
			maybeCallback()
		})
		dispatchErrorMap()
	}

	dispatchErrorMap = func() {
		if opts.GetErrorMap == nil {
			dispatchAuth()
			return
		}

		log.Printf("GetErrorMap sending...")
		a.Encoder.GetErrorMap(d, opts.GetErrorMap, func(errorMap []byte, err error) {
			if currentStage != stageErrorMap {
				return
			}

			if err != nil {
				// when an error occurs, we dont fail bootstrap entirely, we instead
				// return the result indicating no ErrorMap result...
				errorMap = nil
			}

			result.ErrorMap = errorMap
			currentStage = stageAuth
			maybeCallback()
		})
		dispatchAuth()
	}

	dispatchAuth = func() {
		if opts.Auth == nil {
			dispatchSelectBucket()
			return
		}

		log.Printf("Authenticate sending...")
		OpSaslAuthAuto{
			Encoder: a.Encoder,
		}.SASLAuthAuto(d, opts.Auth, func() {
			dispatchSelectBucket()
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
	}

	dispatchSelectBucket = func() {
		if opts.SelectBucket == nil {
			dispatchClusterConfig()
			return
		}

		log.Printf("SelectBucket sending...")
		a.Encoder.SelectBucket(d, opts.SelectBucket, func(err error) {
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
		dispatchClusterConfig()
	}

	dispatchClusterConfig = func() {
		if opts.GetClusterConfig == nil {
			dispatchCallback()
			return
		}

		log.Printf("GetClusterConfig sending...")
		a.Encoder.GetClusterConfig(d, opts.GetClusterConfig, func(clusterConfig []byte, err error) {
			if currentStage != stageClusterConfig {
				return
			}

			if err != nil {
				// when an error occurs, we dont fail bootstrap entirely, we instead
				// return the result indicating no Config result...
				clusterConfig = nil
			}

			result.ClusterConfig = clusterConfig
			currentStage = stageCallback
			maybeCallback()
		})
		dispatchCallback()
	}

	dispatchCallback = func() {}

	maybeCallback()
	if currentStage == stageCallback {
		return
	}

	dispatchHello()
}
