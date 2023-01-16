package core

type kvClientSaslAuthScram struct {
	HashName string
	Username string
	Password string
}

func (a kvClientSaslAuthScram) Authenticate(cli KvClient, cb func(err error)) {
	memdOps{}.SASLAuth(cli, func(_ []byte, err error) {
		cb(err)
	})
}
