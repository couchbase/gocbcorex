package cbqueryx

import (
	"context"
)

type QueryExecutor interface {
	Query(ctx context.Context, opts *QueryOptions) (QueryResultStream, error)
}

type PreparedQuery struct {
	Executor QueryExecutor
	Cache    *PreparedStatementCache
}

func (p PreparedQuery) PreparedQuery(ctx context.Context, opts *QueryOptions) (QueryResultStream, error) {
	newOpts := *opts

	// if this is already marked as auto-execute, we just pass it through
	if opts.AutoExecute {
		return p.Executor.Query(ctx, opts)
	}

	cachedStmt, ok := p.Cache.Get(newOpts.Statement)
	if ok {
		// Attempt to execute our cached query plan
		newOpts.Statement = ""
		newOpts.Prepared = cachedStmt

		res, err := p.Executor.Query(ctx, &newOpts)
		if err == nil {
			return res, nil
		}

		newOpts.Prepared = ""
	}

	newOpts.Statement = "PREPARE " + opts.Statement
	newOpts.AutoExecute = true

	res, err := p.Executor.Query(ctx, &newOpts)
	if err != nil {
		return nil, err
	}

	earlyMetaData := res.EarlyMetaData()
	if earlyMetaData.Prepared != "" {
		p.Cache.Put(opts.Statement, earlyMetaData.Prepared)
	}

	return res, nil
}
