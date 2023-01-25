package core

import (
	"context"

	"github.com/couchbase/stellar-nebula/core/memdx"
)

type CrudComponent struct {
	collections CollectionResolver
	vbuckets    VbucketDispatcher
	retries     RetryManager
	connManager NodeKvClientProvider
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
				Value:    resp.Value,
				Flags:    resp.Flags,
				Datatype: resp.Datatype,
				Cas:      resp.Cas,
			}, nil
		})
}

type UpsertOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
	Value          []byte
	Flags          uint32
	Datatype       uint8
}

type UpsertResult struct {
	Cas uint64
}

func (cc *CrudComponent) Upsert(ctx context.Context, opts *UpsertOptions) (*UpsertResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbuckets, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*UpsertResult, error) {
			resp, err := client.Set(ctx, &memdx.SetRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				Value:        opts.Value,
				Flags:        opts.Flags,
				Datatype:     opts.Datatype,
				Expiry:       0,
			})
			if err != nil {
				return nil, err
			}

			return &UpsertResult{
				Cas: resp.Cas,
			}, nil
		})
}
