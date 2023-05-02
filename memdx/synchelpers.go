package memdx

// this file contains some helpers, mainly used by tests to synchronize
// the asynchronous calls that memdx uses

type unaryResult[T any] struct {
	Resp T
	Err  error
}

func syncUnaryCall[Encoder any, ReqT any, RespT any](
	e Encoder,
	fn func(Encoder, Dispatcher, ReqT, func(RespT, error)) (PendingOp, error),
	d Dispatcher,
	req ReqT,
) (RespT, error) {
	waitCh := make(chan unaryResult[RespT], 1)

	_, err := fn(e, d, req, func(resp RespT, err error) {
		waitCh <- unaryResult[RespT]{
			Resp: resp,
			Err:  err,
		}
	})
	if err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	res := <-waitCh
	return res.Resp, res.Err
}
