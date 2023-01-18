package core

func (agent *FakeAgent) Get(ctx *AsyncContext, opts GetOptions, cb func(*GetResult, error)) error {
	return agent.crud.Get(ctx, opts, cb)
}
