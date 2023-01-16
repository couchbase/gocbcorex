package core

import "errors"

type kvClientSaslAuthByName struct {
	Mechanism string
	Username  string
	Password  string
}

func (a kvClientSaslAuthByName) Authenticate(cli KvClient, cb func(err error)) {
	if a.Mechanism == "PLAIN" {
		kvClientSaslAuthPlain{
			Username: a.Username,
			Password: a.Password,
		}.Authenticate(cli, cb)
	} else if a.Mechanism == "SCRAM-SHA1" {
		kvClientSaslAuthScram{
			HashName: "SHA1",
			Username: a.Username,
			Password: a.Password,
		}.Authenticate(cli, cb)
	} else if a.Mechanism == "SCRAM-SHA256" {
		kvClientSaslAuthScram{
			HashName: "SHA256",
			Username: a.Username,
			Password: a.Password,
		}.Authenticate(cli, cb)
	} else if a.Mechanism == "SCRAM-SHA512" {
		kvClientSaslAuthScram{
			HashName: "SHA512",
			Username: a.Username,
			Password: a.Password,
		}.Authenticate(cli, cb)
	} else {
		// TODO(brett19): Add better error information here
		cb(errors.New("unsupported mechanism"))
	}
}
