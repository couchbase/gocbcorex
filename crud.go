package core

import (
	"context"

	"github.com/couchbase/stellar-nebula/core/memdx"
)

type CrudComponent struct {
	collections   CollectionResolver
	vbuckets      VbucketDispatcher
	retries       RetryComponent
	errorResolver PacketResolver
	connManager   ConnectionManager
}

type GetOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
}

type GetResult struct {
	Value    []byte
	Flags    uint32
	Datatype uint8
	Cas      uint64
}

func (cc *CrudComponent) Get(ctx context.Context, opts *GetOptions) (*GetResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbuckets, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*GetResult, error) {
			resp, err := client.Get(ctx, &memdx.GetRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
			})
			if err != nil {
				return nil, err
			}

			return &GetResult{
				Cas: resp.Cas,
			}, nil
		})
}
