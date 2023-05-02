package memdx

type OpPipeline struct {
	pendingOp    multiPendingOp
	cbQueue      CallbackQueue
	currentStage int
	stages       []opPipelineStage
}

type opPipelineStage interface {
	Schedule()
}

type opPipelineStageTyped[T any] struct {
	Parent *OpPipeline
	Index  int

	Handler func(scheduleNextFn func(), opCb func(res T, err error)) (PendingOp, error)
	Cb      func(res T, err error) bool
}

func (s *opPipelineStageTyped[T]) Schedule() {
	cb := CallbackQueueAdd(&s.Parent.cbQueue, func(res T, err error) {
		if s.Index != s.Parent.currentStage {
			return
		}

		if s.Cb(res, err) {
			s.Parent.currentStage++
		} else {
			s.Parent.currentStage = len(s.Parent.stages)
		}
	})

	op, err := s.Handler(func() {
		s.Parent.scheduleStage(s.Index + 1)
	}, cb)
	if err != nil {
		// this is safe to invoke like this as the callbackqueue will
		// enforce that the order of the actual callback invocation
		// makes sense...
		var emptyResp T
		cb(emptyResp, err)
		return
	}

	s.Parent.pendingOp.Add(op)
}

func OpPipelineAddWithNext[T any](
	p *OpPipeline,
	handler func(nextFn func(), opCb func(res T, err error)) (PendingOp, error),
	cb func(res T, err error) bool,
) {
	item := &opPipelineStageTyped[T]{
		Parent:  p,
		Index:   len(p.stages),
		Handler: handler,
		Cb:      cb,
	}
	p.stages = append(p.stages, item)
}

func OpPipelineAdd[T any](
	p *OpPipeline,
	handler func(opCb func(res T, err error)) (PendingOp, error),
	cb func(res T, err error) bool,
) {
	OpPipelineAddWithNext(p, func(nextFn func(), opCb func(res T, err error)) (PendingOp, error) {
		op, err := handler(opCb)
		nextFn()
		return op, err
	}, cb)
}

func OpPipelineAddSync(
	p *OpPipeline,
	cb func(),
) {
	// we can use OpPipelineAdd as its opCb is protected by an CallbackQueue which ensures
	// that even though we synchronously invoke the opCb, that the actual invocation of that
	// callback does not happen until all previous scheduled callbacks have happened.
	OpPipelineAddWithNext(p, func(nextFn func(), opCb func(res struct{}, err error)) (PendingOp, error) {
		opCb(struct{}{}, nil)
		nextFn()
		return pendingOpNoop{}, nil
	}, func(res struct{}, err error) bool {
		cb()
		return true
	})
}

func (p *OpPipeline) scheduleStage(stageIdx int) {
	if stageIdx >= len(p.stages) {
		return
	}

	stage := p.stages[stageIdx]
	stage.Schedule()
}

func (p *OpPipeline) Start() PendingOp {
	p.scheduleStage(0)
	return &p.pendingOp
}
