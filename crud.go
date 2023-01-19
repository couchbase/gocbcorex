package core

import (
	"errors"
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

			err = cc.vbuckets.DispatchByKey(ctx, opts.Key, func(endpoint string, vbID uint16, err error) {
				if err != nil {
					retry(err)
					return
				}

				err = cc.connManager.Execute(endpoint, func(client KvClient, err error) error {
					if err != nil {
						retry(err)
						return nil
					}

					return memdx.OpsCrud{
						CollectionsEnabled: client.HasFeature(memdx.HelloFeatureCollections), // TODO: update to reflect the truth
					}.Get(client, &memdx.GetRequest{
						CollectionID: cid,
						Key:          opts.Key,
						VbucketID:    vbID,
					}, func(resp *memdx.GetResponse, err error) {
						// if err != nil {
						// 	retry(err)
						// 	return
						// }
						// err = cc.errorResolver.ResolvePacket(resp)
						if err != nil {
							var collectionNotFoundError CollectionNotFoundError
							if errors.As(err, &collectionNotFoundError) {
								cc.collections.InvalidateCollectionID(ctx, opts.ScopeName, opts.CollectionName, endpoint, collectionNotFoundError.ManifestUid)
							}
							retry(err)
							return
						}

						cb(&GetResult{
							Value:    resp.Value,
							Flags:    resp.Flags,
							Datatype: resp.Datatype,
							Cas:      resp.Cas,
						}, nil)
					})
				})
				if err != nil {
					retry(err)
					return
				}
			})
			if err != nil {
				retry(err)
				return
			}
		})
	})
}

func encodeCidIntoKey(key []byte, cid uint32) []byte {
	return []byte{}
}
