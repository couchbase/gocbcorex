package transactionsx

import (
	"context"
	"log"
)

type TransactionCleanupQueue struct {
	cleaner  *TransactionCleaner
	requests chan *TransactionCleanupRequest

	doneSigCh chan struct{}
}

func NewTransactionsCleanupQueue(
	cleaner *TransactionCleaner,
	queueSize uint,
) *TransactionCleanupQueue {
	q := &TransactionCleanupQueue{
		cleaner:  cleaner,
		requests: make(chan *TransactionCleanupRequest, queueSize),

		doneSigCh: make(chan struct{}),
	}

	go q.runThread()

	return q
}

func (q *TransactionCleanupQueue) AddRequest(req *TransactionCleanupRequest) {
	select {
	case q.requests <- req:
		// success!
	default:
		log.Printf("DEBUG: Not queueing request for: %+v, queue length limit reached",
			req)
	}
}

func (q *TransactionCleanupQueue) runThread() {
	ctx := context.Background()

	for req := range q.requests {
		log.Printf("SCHED: Running cleanup for request: %+v", req)
		err := q.cleaner.CleanupAttempt(ctx, req)
		if err != nil {
			log.Printf("DEBUG: Cleanup attempt failed for entry: %+v with error %s",
				req, err)
		}
	}

	close(q.doneSigCh)
}

func (q *TransactionCleanupQueue) GracefulShutdown(ctx context.Context) error {
	close(q.requests)
	select {
	case <-q.doneSigCh:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
