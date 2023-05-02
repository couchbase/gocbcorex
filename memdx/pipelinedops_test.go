package memdx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type opPipelineTestRecord struct {
	ID  int
	Res int
	Err error
}

func TestPipelinedOpsSimple(t *testing.T) {
	var invocations []opPipelineTestRecord

	pipeline := &OpPipeline{}

	OpPipelineAddWithNext(pipeline, func(nextFn func(), opCb func(res int, err error)) (PendingOp, error) {
		go opCb(10, nil)
		nextFn()
		return nil, nil
	}, func(res int, err error) bool {
		invocations = append(invocations, opPipelineTestRecord{
			ID:  1,
			Res: res,
			Err: err,
		})
		return true
	})

	OpPipelineAddWithNext(pipeline, func(nextFn func(), opCb func(res int, err error)) (PendingOp, error) {
		go opCb(20, nil)
		nextFn()
		return nil, nil
	}, func(res int, err error) bool {
		invocations = append(invocations, opPipelineTestRecord{
			ID:  2,
			Res: res,
			Err: err,
		})
		return true
	})

	OpPipelineAddWithNext(pipeline, func(nextFn func(), opCb func(res int, err error)) (PendingOp, error) {
		go opCb(30, nil)
		nextFn()
		return nil, nil
	}, func(res int, err error) bool {
		invocations = append(invocations, opPipelineTestRecord{
			ID:  3,
			Res: res,
			Err: err,
		})
		return true
	})

	waitCh := make(chan struct{}, 1)
	OpPipelineAddSync(pipeline, func() {
		waitCh <- struct{}{}
	})

	pipeline.Start()
	<-waitCh

	assert.Equal(t, []opPipelineTestRecord{
		{ID: 1, Res: 10, Err: nil},
		{ID: 2, Res: 20, Err: nil},
		{ID: 3, Res: 30, Err: nil},
	}, invocations)
}

func TestPipelinedOpsSingleSync(t *testing.T) {
	pipeline := &OpPipeline{}

	waitCh := make(chan struct{}, 1)
	OpPipelineAddSync(pipeline, func() {
		waitCh <- struct{}{}
	})

	pipeline.Start()
	<-waitCh
}
