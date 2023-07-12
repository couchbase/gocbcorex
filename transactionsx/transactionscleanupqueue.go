package transactionsx

import (
	"context"

	"go.uber.org/zap"
)

type TransactionCleanupQueue struct {
	logger   *zap.Logger
	cleaner  *TransactionCleaner
	requests chan *TransactionCleanupRequest

	doneSigCh chan struct{}
}

func NewTransactionsCleanupQueue(
	logger *zap.Logger,
	cleaner *TransactionCleaner,
	queueSize uint,
) *TransactionCleanupQueue {
	q := &TransactionCleanupQueue{
		logger:   logger,
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
		q.logger.Warn("not queueing request, queue limit reached",
			zap.Any("req", req))
	}
}

func (q *TransactionCleanupQueue) runThread() {
	ctx := context.Background()

	for req := range q.requests {
		q.logger.Debug("cleanup queue running cleanup on request",
			zap.Any("req", req))

		err := q.cleaner.CleanupAttempt(ctx, req)
		if err != nil {
			q.logger.Debug("cleanup queue cleanup failed",
				zap.Error(err),
				zap.Any("req", req))
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
