package memdx

import (
	"crypto"
	"errors"
)

type OpSaslAuthByNameEncoder interface {
	OpSaslAuthInvalidEncoder
	OpSaslAuthPlainEncoder
	OpSaslAuthScramEncoder
}

type OpSaslAuthByName struct {
	Encoder OpSaslAuthByNameEncoder
}

type SaslAuthByNameOptions struct {
	Mechanism AuthMechanism
	Username  string
	Password  string
}

func (a OpSaslAuthByName) SASLAuthByName(d Dispatcher, opts *SaslAuthByNameOptions, pipelineCb func(), cb func(err error)) (PendingOp, error) {
	if opts.Mechanism == PlainAuthMechanism {
		return OpSaslAuthPlain{
			Encoder: a.Encoder,
		}.SASLAuthPlain(d, &SaslAuthPlainOptions{
			Username: opts.Username,
			Password: opts.Password,
		}, pipelineCb, cb)
	} else if opts.Mechanism == ScramSha1AuthMechanism {
		return OpSaslAuthScram{
			Encoder: a.Encoder,
		}.SASLAuthScram(d, &SaslAuthScramOptions{
			Hash:     crypto.SHA1,
			Username: opts.Username,
			Password: opts.Password,
		}, pipelineCb, cb)
	} else if opts.Mechanism == ScramSha256AuthMechanism {
		return OpSaslAuthScram{
			Encoder: a.Encoder,
		}.SASLAuthScram(d, &SaslAuthScramOptions{
			Hash:     crypto.SHA256,
			Username: opts.Username,
			Password: opts.Password,
		}, pipelineCb, cb)
	} else if opts.Mechanism == ScramSha512AuthMechanism {
		return OpSaslAuthScram{
			Encoder: a.Encoder,
		}.SASLAuthScram(d, &SaslAuthScramOptions{
			Hash:     crypto.SHA512,
			Username: opts.Username,
			Password: opts.Password,
		}, pipelineCb, cb)
	} else if opts.Mechanism == "INVALID" {
		return OpSaslAuthInvalid{
			Encoder: a.Encoder,
		}.SASLAuthInvalid(d, pipelineCb, cb)
	} else {
		// TODO(brett19): Add better error information here
		return nil, errors.New("unsupported mechanism")
	}
}
