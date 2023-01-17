package memdx

import (
	"errors"
)

type OpSaslAuthPlain struct {
	Username string
	Password string
}

func (a OpSaslAuthPlain) Authenticate(d Dispatcher, pipelineCb func(), cb func(err error)) {
	userBuf := []byte(a.Username)
	passBuf := []byte(a.Password)
	authData := make([]byte, 1+len(userBuf)+1+len(passBuf))
	authData[0] = 0
	copy(authData[1:], userBuf)
	authData[1+len(userBuf)] = 0
	copy(authData[1+len(userBuf)+1:], passBuf)

	OpsCore{}.SASLAuth(d, &SASLAuthRequest{
		Mechanism: PlainAuthMechanism,
		Payload:   authData,
	}, func(resp *SASLAuthResponse, err error) {
		if err != nil {
			cb(err)
			return
		}

		if resp.NeedsMoreSteps {
			cb(errors.New("unexpected PLAIN auth step request"))
			return
		}

		cb(err)
	})

	if pipelineCb != nil {
		pipelineCb()
	}
}
