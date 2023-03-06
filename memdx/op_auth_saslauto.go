package memdx

import (
	"errors"

	"golang.org/x/exp/slices"
)

type OpSaslAuthAutoEncoder interface {
	OpSaslAuthByNameEncoder
	SASLListMechs(Dispatcher, func(*SASLListMechsResponse, error)) (PendingOp, error)
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
		// TODO(brett19): Enhance this error with more details
		return nil, errors.New("must specify at least one allowed authentication mechanism")
	}

	// NOTE(brett19): The following logic is dependant on operation ordering that
	// is guarenteed by memcached, even when Out-Of-Order Execution is enabled.

	pendingOp := &multiPendingOp{}

	op, err := a.Encoder.SASLListMechs(d, func(resp *SASLListMechsResponse, err error) {
		if err != nil {
			// log.Printf("failed to list available authentication mechanisms: %s", err)
			return
		}

		serverMechs = resp.AvailableMechs
	})
	if err == nil {
		pendingOp.Add(op)
	}

	// the default mech is the first one in the list
	defaultMech := opts.EnabledMechs[0]

	op, err = OpSaslAuthByName{
		Encoder: a.Encoder,
	}.SASLAuthByName(d, &SaslAuthByNameOptions{
		Mechanism: defaultMech,
		Username:  opts.Username,
		Password:  opts.Password,
	}, pipelineCb, func(err error) {
		if err != nil {
			if (OpBootstrap{}.isRequestCancelledError(err)) {
				cb(err)
				return
			}
			// TODO(brett19): We should investigate invalid mechanism error handling.
			// There was no obvious way to differentiate between a mechanism being unsupported
			// and the credentials being wrong.  So for now we just assume any error should be
			// ignored if our list-mechs doesn't include it.

			// if we support the default mech, it means this error is 'real', otherwise we try
			// with one of the mechanisms that we now know are supported
			supportsDefaultMech := slices.Contains(serverMechs, defaultMech)
			if supportsDefaultMech {
				cb(err)
				return
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
				// TODO(brett19): Enhance this error with more information
				cb(errors.New("no compatible mechanism was found"))
				return
			}

			op, err := OpSaslAuthByName{
				Encoder: a.Encoder,
			}.SASLAuthByName(d, &SaslAuthByNameOptions{
				Mechanism: selectedMech,
				Username:  opts.Username,
				Password:  opts.Password,
			}, pipelineCb, cb)
			if err != nil {
				cb(err)
				return
			}

			pendingOp.Add(op)
		}

		cb(nil)
	})
	if err != nil {
		return nil, err
	}

	pendingOp.Add(op)

	return op, nil
}
