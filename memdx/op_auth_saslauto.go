package memdx

import (
	"errors"
	"fmt"

	"golang.org/x/exp/slices"
)

type OpSaslAuthAutoEncoder interface {
	OpSaslAuthByNameEncoder
	SASLListMechs(Dispatcher, *SASLListMechsRequest, func(*SASLListMechsResponse, error)) (PendingOp, error)
}

type OpSaslAuthAuto struct {
	Encoder OpSaslAuthAutoEncoder
}

type SaslAuthAutoOptions struct {
	Username string
	Password string

	// EnabledMechs lists the mechanisms that are allowed to be used for
	// authentication.  Earlier entries in the array are selected with higher
	// priority then later entries, with the first entry being unambiguously
	// attempted for non-blocking authentication.
	EnabledMechs []AuthMechanism
}

func (a OpSaslAuthAuto) SASLAuthAuto(d Dispatcher, opts *SaslAuthAutoOptions, pipelineCb func(), cb func(err error)) (PendingOp, error) {
	var serverMechs []AuthMechanism

	if len(opts.EnabledMechs) == 0 {
		return nil, errors.New("must specify at least one allowed authentication mechanism")
	}

	pipeline := OpPipeline{}

	OpPipelineAdd(&pipeline, func(opCb func(res *SASLListMechsResponse, err error)) (PendingOp, error) {
		return a.Encoder.SASLListMechs(d, &SASLListMechsRequest{}, opCb)
	}, func(resp *SASLListMechsResponse, err error) bool {
		if err != nil {
			// when an error occurs, we dont fail authentication entirely, we instead
			// return the result indicating no mechanisms instead...
		} else {
			serverMechs = resp.AvailableMechs
		}

		// this always returns true, so this part of the pipeline would never
		// be retried...
		return true
	})

	// the default mech is the first one in the list
	defaultMech := opts.EnabledMechs[0]

	var secondaryMech AuthMechanism
	OpPipelineAddWithNext(&pipeline, func(nextFn func(), opCb func(res struct{}, err error)) (PendingOp, error) {
		return OpSaslAuthByName{
			Encoder: a.Encoder,
		}.SASLAuthByName(d, &SaslAuthByNameOptions{
			Mechanism: defaultMech,
			Username:  opts.Username,
			Password:  opts.Password,
		}, pipelineCb, func(err error) {
			opCb(struct{}{}, err)

			// we can only move on to the next operation in the pipeline once this one
			// has completely resolved...
			nextFn()
		})
	}, func(res struct{}, err error) bool {
		if err != nil {
			if (OpBootstrap{}.isRequestCancelledError(err)) {
				cb(err)
				return false
			}

			// There is no obvious way to differentiate between a mechanism being unsupported
			// and the credentials being wrong.  So for now we just assume any error should be
			// ignored if our list-mechs doesn't include the mechanism we used.
			// If the server supports the default mech, it means this error is 'real', otherwise
			// we try with one of the mechanisms that we now know are supported
			supportsDefaultMech := slices.Contains(serverMechs, defaultMech)
			if supportsDefaultMech {
				cb(err)
				return false
			}

			foundCompatibleMech := false
			var selectedMech AuthMechanism
			for _, mech := range opts.EnabledMechs {
				if slices.Contains(serverMechs, mech) {
					foundCompatibleMech = true
					selectedMech = mech
					break
				}
			}

			if !foundCompatibleMech {
				cb(fmt.Errorf(
					"no supported auth mechanism was found (enabled: %v, server: %v)",
					opts.EnabledMechs,
					serverMechs,
				))
				return false
			}

			secondaryMech = selectedMech
			return true
		}

		// if we were successful in authenticating, we don't need to continue to the secondary mech
		// attempt and instead can immediately call back and avoid the rest of the pipeline.
		cb(nil)
		return false
	})

	// we add a final pipelined operation which attempts to perform authentication using the secondary
	// mechanism if one is available.
	OpPipelineAdd(&pipeline, func(opCb func(res struct{}, err error)) (PendingOp, error) {
		return OpSaslAuthByName{
			Encoder: a.Encoder,
		}.SASLAuthByName(d, &SaslAuthByNameOptions{
			Mechanism: secondaryMech,
			Username:  opts.Username,
			Password:  opts.Password,
		}, pipelineCb, cb)
	}, func(res struct{}, err error) bool {
		if err != nil {
			cb(err)
			return false
		}

		return true
	})

	OpPipelineAddSync(&pipeline, func() {
		cb(nil)
	})

	return pipeline.Start(), nil
}
