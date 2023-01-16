package core

type kvClientSaslAuthPlain struct {
	Username string
	Password string
}

func (a kvClientSaslAuthPlain) Authenticate(cli KvClient, cb func(err error)) {
	memdOps{}.SASLAuth(cli, func(_ []byte, err error) {
		cb(err)
	})
}
