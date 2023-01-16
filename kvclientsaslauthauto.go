package core

import (
	"errors"
	"sync"

	"golang.org/x/exp/slices"
)

type kvClientSaslAuthAuto struct {
	Username string
	Password string

	// EnabledMechs lists the mechanisms that are allowed to be used for
	// authentication.  Earlier entries in the array are selected with higher
	// priority then later entries, with the first entry being unambiguously
	// attempted for non-blocking authentication.
	EnabledMechs []string
}

func (a *kvClientSaslAuthAuto) Authenticate(cli KvClient, cb func(err error)) {
	var mechsWaitGroup sync.WaitGroup
	var mechsListErr error
	var serverMechs []string

	if len(a.EnabledMechs) == 0 {
		// TODO(brett19): Enhance this error with more details
		cb(errors.New("must specify at least one allowed authentication mechanism"))
		return
	}

	mechsWaitGroup.Add(1)
	memdOps{}.SASLListMechs(cli, func(mechs []string, err error) {
		if err != nil {
			mechsListErr = err
			mechsWaitGroup.Done()
			return
		}

		serverMechs = mechs
		mechsWaitGroup.Done()
	})

	kvClientSaslAuthByName{
		Mechanism: a.EnabledMechs[0],
		Username:  a.Username,
		Password:  a.Password,
	}.Authenticate(cli, func(err error) {
		if err != nil {
			// TODO(brett19): Implement proper checking of these
			isInvalidMechanismError := false
			if isInvalidMechanismError {
				mechsWaitGroup.Wait()

				// if we failed to list the mechanisms that are available,
				// return that error directly for easier debugging.
				if mechsListErr != nil {
					cb(mechsListErr)
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
				}

				kvClientSaslAuthByName{
					Mechanism: selectedMech,
					Username:  a.Username,
					Password:  a.Password,
				}.Authenticate(cli, cb)
				return
			} else {
				cb(err)
				return
			}
		}

		cb(nil)
	})
}
