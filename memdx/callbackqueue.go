package memdx

import "sync"

// CallbackQueue implements a structure which enables guarenteed ordering
// of callbacks that are being invoked.  This is used for ensuring that some
// important operation ordering is maintained even in the face of unordered
// operation failures (like cancellation).
// Note that we need to use a lock here as it is possible that new operations
// get scheduled while we are in the midst of processing other operations,
// for instance in the OpPipeline case where we can be scheduling the initial
// set of operations while those operations are completing on an IO thread.
type CallbackQueue struct {
	currentIndex int
	queue        []callbackQueueItem
	lock         sync.Mutex
}

type callbackQueueItem interface {
	MaybeInvoke()
}

type callbackQueueItemTyped[T any] struct {
	Parent *CallbackQueue
	Index  int

	Cb        func(resp T, err error)
	HasResult bool
	Result    T
	Err       error
}

func (i *callbackQueueItemTyped[T]) Callback(res T, err error) {
	i.Parent.lock.Lock()
	if i.HasResult {
		// This is technically an error condition, we handle it secretly by
		// just ignoring any later callback invocations.
		i.Parent.lock.Unlock()
		return
	}

	i.HasResult = true
	i.Result = res
	i.Err = err
	i.Parent.lock.Unlock()

	i.MaybeInvoke()
}

func (i *callbackQueueItemTyped[T]) MaybeInvoke() {
	i.Parent.lock.Lock()
	if !i.HasResult {
		i.Parent.lock.Unlock()
		return
	}

	if i.Parent.currentIndex != i.Index {
		i.Parent.lock.Unlock()
		return
	}

	i.Cb(i.Result, i.Err)
	i.Parent.currentIndex++

	currentIndex := i.Parent.currentIndex
	if currentIndex >= len(i.Parent.queue) {
		i.Parent.lock.Unlock()
		return
	}

	nextItem := i.Parent.queue[i.Parent.currentIndex]
	i.Parent.lock.Unlock()

	nextItem.MaybeInvoke()
}

func CallbackQueueAdd[T any](q *CallbackQueue, cb func(resp T, err error)) func(resp T, err error) {
	q.lock.Lock()
	item := &callbackQueueItemTyped[T]{
		Parent: q,
		Index:  len(q.queue),
		Cb:     cb,
	}
	q.queue = append(q.queue, item)
	q.lock.Unlock()

	return item.Callback
}
