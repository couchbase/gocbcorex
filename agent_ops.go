package core

import "context"

func (agent *Agent) Upsert(ctx context.Context, opts *UpsertOptions) (*UpsertResult, error) {
	return agent.crud.Upsert(ctx, opts)
}

func (agent *Agent) Get(ctx context.Context, opts *GetOptions) (*GetResult, error) {
	return agent.crud.Get(ctx, opts)
}

func (agent *Agent) Delete(ctx context.Context, opts *DeleteOptions) (*DeleteResult, error) {
	return agent.crud.Delete(ctx, opts)
}

func (agent *Agent) SendHTTPRequest(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	return agent.http.SendHTTPRequest(ctx, req)
}

func (agent *Agent) Query(ctx context.Context, opts *QueryOptions) (*QueryRowReader, error) {
	return agent.query.Query(ctx, opts)
}

func (agent *Agent) PreparedQuery(ctx context.Context, opts *QueryOptions) (*QueryRowReader, error) {
	return agent.query.PreparedQuery(ctx, opts)
}
