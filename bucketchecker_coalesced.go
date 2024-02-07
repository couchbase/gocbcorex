package gocbcorex

import (
	"context"
	"errors"
	"sync"
)

type bucketCheck struct {
	CheckRes   bool
	CheckErr   error
	NeedsCheck bool
	PendingCh  chan struct{}
}

type BucketCheckerCoalesced struct {
	checker BucketChecker
	lock    sync.Mutex
	buckets map[string]*bucketCheck
}

var _ BucketChecker = (*BucketCheckerCoalesced)(nil)

type BucketCheckerCoalescedOptions struct {
	Checker BucketChecker
}

func NewBucketCheckerCoalesced(opts *BucketCheckerCoalescedOptions) *BucketCheckerCoalesced {
	return &BucketCheckerCoalesced{
		checker: opts.Checker,
		buckets: make(map[string]*bucketCheck),
	}
}

func (m *BucketCheckerCoalesced) bucketCheckThread(bucketName string, bucket *bucketCheck) {
	m.lock.Lock()
	nextCheck := &bucketCheck{
		NeedsCheck: false,
		PendingCh:  make(chan struct{}),
	}
	m.buckets[bucketName] = nextCheck
	m.lock.Unlock()

	resp, err := m.checker.HasBucket(context.Background(), bucketName)

	m.lock.Lock()

	bucket.CheckRes = resp
	bucket.CheckErr = err

	pendingCh := bucket.PendingCh
	bucket.PendingCh = nil

	nextNeedsCheck := nextCheck.NeedsCheck

	if !nextNeedsCheck {
		delete(m.buckets, bucketName)
	}

	m.lock.Unlock()

	close(pendingCh)

	if nextNeedsCheck {
		m.bucketCheckThread(bucketName, nextCheck)
	}
}

func (m *BucketCheckerCoalesced) HasBucket(ctx context.Context, bucketName string) (bool, error) {
	m.lock.Lock()

	var pendingCh chan struct{}

	check := m.buckets[bucketName]
	if check == nil {
		check = &bucketCheck{
			PendingCh:  make(chan struct{}),
			NeedsCheck: true,
		}
		m.buckets[bucketName] = check

		pendingCh = check.PendingCh

		m.lock.Unlock()

		go m.bucketCheckThread(bucketName, check)
	} else {
		check.NeedsCheck = true
		pendingCh = check.PendingCh

		m.lock.Unlock()
	}

	if pendingCh == nil {
		return false, errors.New("bucket checker pending channel was invalid")
	}

	select {
	case <-pendingCh:
		return check.CheckRes, check.CheckErr
	case <-ctx.Done():
		return false, ctx.Err()
	}
}
