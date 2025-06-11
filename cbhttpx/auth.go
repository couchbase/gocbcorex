package cbhttpx

import "net/http"

type Authenticator interface {
	ApplyToRequest(req *http.Request)
}

type BasicAuth struct {
	Username string
	Password string
}

var _ Authenticator = BasicAuth{}

func (b BasicAuth) ApplyToRequest(req *http.Request) {
	req.SetBasicAuth(b.Username, b.Password)
}
