package memdx

import (
	"crypto"
	"errors"

	"github.com/couchbase/gocbcorex/scram"
)

type OpSaslAuthScramEncoder interface {
	SASLAuth(Dispatcher, *SASLAuthRequest, func(*SASLAuthResponse, error)) (PendingOp, error)
	SASLStep(Dispatcher, *SASLStepRequest, func(*SASLStepResponse, error)) (PendingOp, error)
}

type OpSaslAuthScram struct {
	Encoder OpSaslAuthScramEncoder
}

type SaslAuthScramOptions struct {
	Hash     crypto.Hash
	Username string
	Password string
}

func (a OpSaslAuthScram) SASLAuthScram(d Dispatcher, req *SaslAuthScramOptions, pipelineCb func(), cb func(err error)) (PendingOp, error) {
	var mechName AuthMechanism
	switch req.Hash {
	case crypto.SHA1:
		mechName = ScramSha1AuthMechanism
	case crypto.SHA256:
		mechName = ScramSha256AuthMechanism
	case crypto.SHA512:
		mechName = ScramSha512AuthMechanism
	}
	if mechName == "" {
		return nil, errors.New("unsupported hash type: " + req.Hash.String())
	}

	scramMgr := scram.NewClient(req.Hash.New, req.Username, req.Password)

	pendingOp := &multiPendingOp{}

	// Perform the initial SASL step
	scramMgr.Step(nil)
	op, err := a.Encoder.SASLAuth(d, &SASLAuthRequest{
		Mechanism: mechName,
		Payload:   scramMgr.Out(),
	}, func(resp *SASLAuthResponse, err error) {
		if err != nil {
			cb(err)
			return
		}

		if !resp.NeedsMoreSteps {
			// log.Printf("WARN: server accepted auth before client expected")
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

		op, err := a.Encoder.SASLStep(d, &SASLStepRequest{
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
		if err != nil {
			cb(err)
			return
		}

		pendingOp.Add(op)

		if pipelineCb != nil {
			pipelineCb()
		}
	})
	if err != nil {
		return nil, err
	}

	pendingOp.Add(op)

	return pendingOp, nil
}
