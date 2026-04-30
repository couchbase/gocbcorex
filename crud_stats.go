package gocbcorex

import (
	"context"
	"sync"

	"github.com/couchbase/gocbcorex/memdx"
)

type StatsResult struct{}

type StatsDataResult struct {
	Key   string
	Value string
}

type StatsByVbucketOptions struct {
	GroupName  string
	VbucketID  uint16
	OnBehalfOf string
}

type StatsByKeyOptions struct {
	GroupName  string
	OnBehalfOf string
}

func (cc *CrudComponent) StatsByVbucket(
	ctx context.Context,
	opts *StatsByVbucketOptions,
	dataCb func(StatsDataResult),
) (*StatsResult, error) {
	ctx, span := tracer.Start(ctx, "Stats")
	defer span.End()

	return OrchestrateSimpleVbCrud(
		ctx, cc.retries, cc.vbs, cc.nmvHandler, cc.eclientProvider,
		opts.VbucketID, 0,
		func(endpoint string, client KvClient) (*StatsResult, error) {
			_, err := client.Stats(ctx, &memdx.StatsRequest{
				GroupName: opts.GroupName,
				UtilsRequestMeta: memdx.UtilsRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
			}, func(resp *memdx.StatsDataResponse) error {
				dataCb(StatsDataResult{
					Key:   resp.Key,
					Value: resp.Value,
				})
				return nil
			})
			if err != nil {
				return nil, err
			}

			return &StatsResult{}, nil
		})
}

// StatsByKey sends the STAT command with the given group name to all KV nodes
// in parallel. dataCb is called serially (under a mutex) for each entry
// received across all nodes; callers are responsible for aggregating results
// across nodes (e.g. summing per-collection counts).
func (cc *CrudComponent) StatsByKey(
	ctx context.Context,
	opts *StatsByKeyOptions,
	dataCb func(StatsDataResult),
) error {
	ctx, span := tracer.Start(ctx, "StatsByKey")
	defer span.End()

	servers, err := cc.vbs.GetServerList()
	if err != nil {
		return err
	}

	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup

	for _, endpoint := range servers {
		wg.Add(1)
		go func(ep string) {
			defer wg.Done()
			_, err := OrchestrateRetries(ctx, cc.retries, func() (struct{}, error) {
				return OrchestrateEndpointKvClient(ctx, cc.eclientProvider, ep, func(client KvClient) (struct{}, error) {
					_, err := client.Stats(ctx, &memdx.StatsRequest{
						GroupName: opts.GroupName,
						UtilsRequestMeta: memdx.UtilsRequestMeta{
							OnBehalfOf: opts.OnBehalfOf,
						},
					}, func(resp *memdx.StatsDataResponse) error {
						mu.Lock()
						dataCb(StatsDataResult{Key: resp.Key, Value: resp.Value})
						mu.Unlock()
						return nil
					})
					return struct{}{}, err
				})
			})
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(endpoint)
	}

	wg.Wait()
	return firstErr
}
