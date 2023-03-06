package memdx

// OpSaslAuthInvalid exists to support some testing requirements
// and intentionally executes a SASLAuth that is known to be invalid.
type OpSaslAuthInvalidEncoder interface {
	SASLAuth(Dispatcher, *SASLAuthRequest, func(*SASLAuthResponse, error)) (PendingOp, error)
}

type OpSaslAuthInvalid struct {
	Encoder OpSaslAuthInvalidEncoder
}

func (a OpSaslAuthInvalid) SASLAuthInvalid(d Dispatcher, pipelineCb func(), cb func(err error)) (PendingOp, error) {
	op, err := a.Encoder.SASLAuth(d, &SASLAuthRequest{
		Mechanism: "INVALID",
		Payload:   []byte{0, 'a', 'b', 'c', 0, 'a', 'b', 'c'},
	}, func(resp *SASLAuthResponse, err error) {
		cb(err)
	})
	if err != nil {
		return nil, err
	}

	if pipelineCb != nil {
		pipelineCb()
	}

	return op, nil
}
