package memdx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type callbackQueueTestRecord struct {
	ID  int
	Res int
	Err error
}

func TestCallbackQueueSimple(t *testing.T) {
	var invocations []callbackQueueTestRecord

	q := &CallbackQueue{}
	cb1 := CallbackQueueAdd(q, func(res int, err error) {
		invocations = append(invocations, callbackQueueTestRecord{
			ID:  1,
			Res: res,
			Err: err,
		})
	})
	cb2 := CallbackQueueAdd(q, func(res int, err error) {
		invocations = append(invocations, callbackQueueTestRecord{
			ID:  2,
			Res: res,
			Err: err,
		})
	})
	cb3 := CallbackQueueAdd(q, func(res int, err error) {
		invocations = append(invocations, callbackQueueTestRecord{
			ID:  3,
			Res: res,
			Err: err,
		})
	})

	cb1(10, nil)
	cb2(20, nil)
	cb3(30, nil)

	assert.Equal(t, []callbackQueueTestRecord{
		{ID: 1, Res: 10, Err: nil},
		{ID: 2, Res: 20, Err: nil},
		{ID: 3, Res: 30, Err: nil},
	}, invocations)
}

func TestCallbackQueueSingle(t *testing.T) {
	var invocations []callbackQueueTestRecord

	q := &CallbackQueue{}
	cb1 := CallbackQueueAdd(q, func(res int, err error) {
		invocations = append(invocations, callbackQueueTestRecord{
			ID:  1,
			Res: res,
			Err: err,
		})
	})

	cb1(10, nil)

	assert.Equal(t, []callbackQueueTestRecord{
		{ID: 1, Res: 10, Err: nil},
	}, invocations)
}

func TestCallbackQueueBackwards(t *testing.T) {
	var invocations []callbackQueueTestRecord

	q := &CallbackQueue{}
	cb1 := CallbackQueueAdd(q, func(res int, err error) {
		invocations = append(invocations, callbackQueueTestRecord{
			ID:  1,
			Res: res,
			Err: err,
		})
	})
	cb2 := CallbackQueueAdd(q, func(res int, err error) {
		invocations = append(invocations, callbackQueueTestRecord{
			ID:  2,
			Res: res,
			Err: err,
		})
	})
	cb3 := CallbackQueueAdd(q, func(res int, err error) {
		invocations = append(invocations, callbackQueueTestRecord{
			ID:  3,
			Res: res,
			Err: err,
		})
	})

	cb3(30, nil)
	cb2(20, nil)
	cb1(10, nil)

	assert.Equal(t, []callbackQueueTestRecord{
		{ID: 1, Res: 10, Err: nil},
		{ID: 2, Res: 20, Err: nil},
		{ID: 3, Res: 30, Err: nil},
	}, invocations)
}
