package memdx

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"hash"
	"log"

	"github.com/couchbase/stellar-nebula/core/scram"
)

type OpSaslAuthScram struct {
	HashName string
	Username string
	Password string
}

func (a OpSaslAuthScram) getHashNew() (func() hash.Hash, error) {
	switch a.HashName {
	case "SHA1":
		return sha1.New, nil
	case "SHA256":
		return sha256.New, nil
	case "SHA512":
		return sha512.New, nil
	}

	return nil, errors.New("invalid hash name: " + a.HashName)
}

func (a OpSaslAuthScram) Authenticate(d Dispatcher, cb func(err error)) {
	mechName := "SCRAM-" + a.HashName

	hash, err := a.getHashNew()
	if err != nil {
		cb(err)
		return
	}

	scramMgr := scram.NewClient(hash, a.Username, a.Password)

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
	})
}
