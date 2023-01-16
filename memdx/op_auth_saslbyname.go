package memdx

import "errors"

type OpSaslAuthByName struct {
	Mechanism string
	Username  string
	Password  string
}

func (a OpSaslAuthByName) Authenticate(d Dispatcher, cb func(err error)) {
	if a.Mechanism == "PLAIN" {
		OpSaslAuthPlain{
			Username: a.Username,
			Password: a.Password,
		}.Authenticate(d, cb)
	} else if a.Mechanism == "SCRAM-SHA1" {
		OpSaslAuthScram{
			HashName: "SHA1",
			Username: a.Username,
			Password: a.Password,
		}.Authenticate(d, cb)
	} else if a.Mechanism == "SCRAM-SHA256" {
		OpSaslAuthScram{
			HashName: "SHA256",
			Username: a.Username,
			Password: a.Password,
		}.Authenticate(d, cb)
	} else if a.Mechanism == "SCRAM-SHA512" {
		OpSaslAuthScram{
			HashName: "SHA512",
			Username: a.Username,
			Password: a.Password,
		}.Authenticate(d, cb)
	} else {
		// TODO(brett19): Add better error information here
		cb(errors.New("unsupported mechanism"))
	}
}
