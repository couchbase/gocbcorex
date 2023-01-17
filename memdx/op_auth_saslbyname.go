package memdx

import (
	"crypto"
	"errors"
)

type OpSaslAuthByName struct {
	Mechanism string
	Username  string
	Password  string
}

func (a OpSaslAuthByName) Authenticate(d Dispatcher, pipelineCb func(), cb func(err error)) {
	if a.Mechanism == "PLAIN" {
		OpSaslAuthPlain{
			Username: a.Username,
			Password: a.Password,
		}.Authenticate(d, pipelineCb, cb)
	} else if a.Mechanism == "SCRAM-SHA1" {
		OpSaslAuthScram{
			Hash:     crypto.SHA1,
			Username: a.Username,
			Password: a.Password,
		}.Authenticate(d, pipelineCb, cb)
	} else if a.Mechanism == "SCRAM-SHA256" {
		OpSaslAuthScram{
			Hash:     crypto.SHA256,
			Username: a.Username,
			Password: a.Password,
		}.Authenticate(d, pipelineCb, cb)
	} else if a.Mechanism == "SCRAM-SHA512" {
		OpSaslAuthScram{
			Hash:     crypto.SHA512,
			Username: a.Username,
			Password: a.Password,
		}.Authenticate(d, pipelineCb, cb)
	} else if a.Mechanism == "INVALID" {
		OpSaslAuthInvalid{}.Authenticate(d, pipelineCb, cb)
	} else {
		// TODO(brett19): Add better error information here
		cb(errors.New("unsupported mechanism"))
	}
}
