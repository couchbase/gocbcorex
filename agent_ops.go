package core

import "context"

func (agent *FakeAgent) SyncGet(ctx context.Context, opts *GetOptions) (*GetResult, error) {
	return agent.crud.Get(ctx, opts)
}
