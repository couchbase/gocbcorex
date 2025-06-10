package cbhttpx

import "net/http"

type Authenticator interface {
	applyToRequest(req *http.Request)
}

type BasicAuth struct {
	Username string
	Password string
}

func (b BasicAuth) applyToRequest(req *http.Request) {
	req.SetBasicAuth(b.Username, b.Password)
}
