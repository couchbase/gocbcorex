package core

import (
	"errors"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type CrudComponent struct {
	collections  CollectionResolver
	vbuckets     VbucketDispatcher
	serverRouter ServerDispatcher
	retries      RetryComponent
}

type GetOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
}

type GetResult struct {
}

func (cc *CrudComponent) Get(ctx *AsyncContext, opts GetOptions, cb func(*GetResult, error)) error {
	return cc.retries.OrchestrateRetries(ctx, func(retry func(error), err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		cc.collections.ResolveCollectionID(ctx, "", opts.ScopeName, opts.CollectionName, func(cid uint32, _ uint64, err error) {
			if err != nil {
				retry(err)
				return
			}

			endpoint, err := cc.vbuckets.DispatchByKey(ctx, opts.Key)
			if err != nil {
				retry(err)
				return
			}

			packet := &memd.Packet{
				Key:          encodeCidIntoKey(opts.Key, cid),
				CollectionID: cid,
			}

			cc.serverRouter.DispatchToServer(ctx, endpoint, packet, func(resp *memd.Packet, err error) {
				if err != nil {
					if errors.Is(err, gocbcore.ErrCollectionNotFound) {
						cc.collections.InvalidateCollectionID(ctx, opts.ScopeName, opts.CollectionName, endpoint, 0)
					}
					retry(err)
					return
				}

				cb(&GetResult{}, nil)
			})
		})
	})
}

func encodeCidIntoKey(key []byte, cid uint32) []byte {
	return []byte{}
}
