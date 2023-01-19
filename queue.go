package core

import (
	"sync"

	"golang.org/x/exp/slices"
)

type comparableQueueItem[T any] struct {
	value T
}

// Very naive implementation of a queue
type queue[T comparable] struct {
	items  []T
	size   uint32
	lock   sync.Mutex
	wait   *sync.Cond
	closed bool
}

func newQueue[T comparable](size uint32) *queue[T] {
	q := &queue[T]{
		size: size,
	}
	q.wait = sync.NewCond(&q.lock)

	return q
}

func (q *queue[T]) Next() *T {
	q.lock.Lock()
	if len(q.items) == 0 {
		// There are no items in the queue so wait until there are, or we're closed
		q.wait.Wait()
	}
	if q.closed {
		q.lock.Unlock()
		return nil
	}
	next := q.items[0]
	q.items = q.items[1:]
	q.lock.Unlock()
	return &next
}

func (q *queue[T]) Push(item T) error {
	q.lock.Lock()
	if len(q.items) >= int(q.size) {
		q.lock.Unlock()
		return placeholderError{"too many items"}
	}
	q.items = append(q.items, item)
	q.lock.Unlock()

	// Signal that there are now items
	q.wait.Broadcast()

	return nil
}

func (q *queue[T]) Remove(item T) error {
	q.lock.Lock()
	for i, qItem := range q.items {
		if qItem == item {
			slices.Delete(q.items, i, i+1)
			q.lock.Unlock()
			return nil
		}
	}
	q.lock.Unlock()

	return placeholderError{"unknown item"}
}

func (q *queue[T]) Close() {
	q.lock.Lock()
	q.closed = true
	q.lock.Unlock()

	q.wait.Broadcast()
}
