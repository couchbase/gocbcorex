package memdx

import "sync"

type multiPendingOp struct {
	ops       []PendingOp
	lock      sync.Mutex
	cancelErr error
}

func (op *multiPendingOp) Cancel(err error) {
	if err == nil {
		panic("must specify a cancellation error")
	}

	op.lock.Lock()
	op.cancelErr = err

	// since we guarentee that ops wont be added once cancelErr is set,
	// we can safely reference the existing list of ops for cancelling.
	ops := op.ops

	op.lock.Unlock()

	for _, op := range ops {
		op.Cancel(err)
	}
}

func (op *multiPendingOp) Add(opToAdd PendingOp) {
	op.lock.Lock()
	cancelErr := op.cancelErr
	if cancelErr != nil {
		op.lock.Unlock()
		opToAdd.Cancel(cancelErr)
		return
	}

	op.ops = append(op.ops, opToAdd)
	op.lock.Unlock()
}
