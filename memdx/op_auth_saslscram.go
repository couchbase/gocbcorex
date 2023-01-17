package memdx

import (
	"crypto"
	"errors"
	"log"

	"github.com/couchbase/stellar-nebula/core/scram"
)

type OpSaslAuthScram struct {
	Hash     crypto.Hash
	Username string
	Password string
}

func (a OpSaslAuthScram) Authenticate(d Dispatcher, pipelineCb func(), cb func(err error)) {
	var mechName AuthMechanism
	switch a.Hash {
	case crypto.SHA1:
		mechName = ScramSha1AuthMechanism
	case crypto.SHA256:
		mechName = ScramSha256AuthMechanism
	case crypto.SHA512:
		mechName = ScramSha512AuthMechanism
	}
	if mechName == "" {
		cb(errors.New("unsupported hash type: " + a.Hash.String()))
		return
	}

	scramMgr := scram.NewClient(a.Hash.New, a.Username, a.Password)

	// Perform the initial SASL step
	scramMgr.Step(nil)
	OpsCore{}.SASLAuth(d, &SASLAuthRequest{
		Mechanism: mechName,
		Payload:   scramMgr.Out(),
	}, func(resp *SASLAuthResponse, err error) {
		if err != nil {
			cb(err)
			return
		}

		if !resp.NeedsMoreSteps {
			log.Printf("WARN: server accepted auth before client expected")
			cb(nil)
			return
		}

		if !scramMgr.Step(resp.Payload) {
			err = scramMgr.Err()
			if err != nil {
				cb(err)
				return
			}

			cb(errors.New("local auth client finished before server accepted auth"))
			return
		}

		OpsCore{}.SASLStep(d, &SASLStepRequest{
			Mechanism: mechName,
			Payload:   scramMgr.Out(),
		}, func(resp *SASLStepResponse, err error) {
			if err != nil {
				cb(err)
				return
			}

			if resp.NeedsMoreSteps {
				cb(errors.New("server did not accept auth when the client expected"))
			}

			cb(nil)
		})

		if pipelineCb != nil {
			pipelineCb()
		}
	})
}
