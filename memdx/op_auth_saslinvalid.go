package memdx

// OpSaslAuthInvalid exists to support some testing requirements
// and intentionally executes a SASLAuth that is known to be invalid.
type OpSaslAuthInvalid struct {
}

func (a OpSaslAuthInvalid) Authenticate(d Dispatcher, cb func(err error)) {
	OpsCore{}.SASLAuth(d, &SASLAuthRequest{
		Mechanism: "INVALID",
		Payload:   []byte{0, 'a', 'b', 'c', 0, 'a', 'b', 'c'},
	}, func(resp *SASLAuthResponse, err error) {
		cb(err)
	})
}
