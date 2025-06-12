package memdx

import (
	"crypto"
	"fmt"
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
	switch opts.Mechanism {
	case PlainAuthMechanism:
		return OpSaslAuthPlain{
			Encoder: a.Encoder,
		}.SASLAuthPlain(d, &SaslAuthPlainOptions{
			Username: opts.Username,
			Password: opts.Password,
		}, pipelineCb, cb)
	case ScramSha1AuthMechanism:
		return OpSaslAuthScram{
			Encoder: a.Encoder,
		}.SASLAuthScram(d, &SaslAuthScramOptions{
			Hash:     crypto.SHA1,
			Username: opts.Username,
			Password: opts.Password,
		}, pipelineCb, cb)
	case ScramSha256AuthMechanism:
		return OpSaslAuthScram{
			Encoder: a.Encoder,
		}.SASLAuthScram(d, &SaslAuthScramOptions{
			Hash:     crypto.SHA256,
			Username: opts.Username,
			Password: opts.Password,
		}, pipelineCb, cb)
	case ScramSha512AuthMechanism:
		return OpSaslAuthScram{
			Encoder: a.Encoder,
		}.SASLAuthScram(d, &SaslAuthScramOptions{
			Hash:     crypto.SHA512,
			Username: opts.Username,
			Password: opts.Password,
		}, pipelineCb, cb)
	case "INVALID":
		return OpSaslAuthInvalid{
			Encoder: a.Encoder,
		}.SASLAuthInvalid(d, pipelineCb, cb)
	default:
		return nil, fmt.Errorf("unsupported mechanism specified (mechanism: %s)", opts.Mechanism)
	}
}
