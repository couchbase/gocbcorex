package memdx

import (
	"errors"
	"log"

	"golang.org/x/exp/slices"
)

type OpSaslAuthAuto struct {
	Username string
	Password string

	// EnabledMechs lists the mechanisms that are allowed to be used for
	// authentication.  Earlier entries in the array are selected with higher
	// priority then later entries, with the first entry being unambiguously
	// attempted for non-blocking authentication.
	EnabledMechs []string
}

func (a OpSaslAuthAuto) Authenticate(d Dispatcher, pipelineCb func(), cb func(err error)) {
	var serverMechs []string

	if len(a.EnabledMechs) == 0 {
		// TODO(brett19): Enhance this error with more details
		cb(errors.New("must specify at least one allowed authentication mechanism"))
		return
	}

	// NOTE(brett19): The following logic is dependant on operation ordering that
	// is guarenteed by memcached, even when Out-Of-Order Execution is enabled.

	OpsCore{}.SASLListMechs(d, func(resp *SASLListMechsResponse, err error) {
		if err != nil {
			log.Printf("failed to list available authentication mechanisms: %s", err)
			return
		}

		serverMechs = resp.AvailableMechs
	})

	// the default mech is the first one in the list
	defaultMech := a.EnabledMechs[0]

	OpSaslAuthByName{
		Mechanism: defaultMech,
		Username:  a.Username,
		Password:  a.Password,
	}.Authenticate(d, pipelineCb, func(err error) {
		if err != nil {
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
			selectedMech := ""
			for _, mech := range a.EnabledMechs {
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

			OpSaslAuthByName{
				Mechanism: selectedMech,
				Username:  a.Username,
				Password:  a.Password,
			}.Authenticate(d, pipelineCb, cb)
			return
		}

		cb(nil)
	})
}
