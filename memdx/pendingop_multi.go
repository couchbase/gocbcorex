package memdx

import (
	"errors"
	"sync"
)

type multiPendingOp struct {
	ops       []PendingOp
	lock      sync.Mutex
	cancelErr error
}

func (op *multiPendingOp) Cancel(err error) bool {
	if err == nil {
		err = errors.New("unspecified cancellation error")
	}

	op.lock.Lock()
	op.cancelErr = err

	// since we guarentee that ops wont be added once cancelErr is set,
	// we can safely reference the existing list of ops for cancelling.
	ops := op.ops

	op.lock.Unlock()

	anyCancelled := false
	for _, op := range ops {
		anyCancelled = anyCancelled || op.Cancel(err)
	}

	return anyCancelled
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
