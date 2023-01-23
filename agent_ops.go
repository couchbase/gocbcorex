package core

import "context"

func (agent *FakeAgent) Upsert(ctx context.Context, opts *UpsertOptions) (*UpsertResult, error) {
	return agent.crud.Upsert(ctx, opts)
}

func (agent *FakeAgent) Get(ctx context.Context, opts *GetOptions) (*GetResult, error) {
	return agent.crud.Get(ctx, opts)
}
