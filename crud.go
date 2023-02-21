package gocbcorex

import (
	"context"

	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

type MutationToken struct {
	VbID   uint16
	VbUuid uint64
	SeqNo  uint64
}

type CrudComponent struct {
	logger      *zap.Logger
	collections CollectionResolver
	cfgmanager  ConfigManager
	retries     RetryManager
	connManager KvClientManager
	compression CompressionManager
	vbs         VbucketRouter
}

func OrchestrateSimpleCrud[RespT any](
	ctx context.Context,
	rs RetryManager,
	cr CollectionResolver,
	vb VbucketRouter,
	cm ConfigManager,
	nkcp KvClientManager,
	scopeName, collectionName string,
	key []byte,
	fn func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (RespT, error),
) (RespT, error) {
	return OrchestrateMemdRetries(
		ctx, rs,
		func() (RespT, error) {
			return OrchestrateMemdCollectionID(
				ctx, cr, scopeName, collectionName,
				func(collectionID uint32, manifestID uint64) (RespT, error) {
					return OrchestrateMemdRouting(ctx, vb, cm, key, 0, func(endpoint string, vbID uint16) (RespT, error) {
						return OrchestrateMemdClient(ctx, nkcp, endpoint, func(client KvClient) (RespT, error) {
							return fn(collectionID, manifestID, endpoint, vbID, client)
						})
					})
				})
		})
}

type GetOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
	OnBehalfOf     string
}

type GetResult struct {
	Value    []byte
	Flags    uint32
	Datatype memdx.DatatypeFlag
	Cas      uint64
}

func (cc *CrudComponent) Get(ctx context.Context, opts *GetOptions) (*GetResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*GetResult, error) {
			resp, err := client.Get(ctx, &memdx.GetRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				OnBehalfOf:   opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			value, datatype, err := cc.compression.Decompress(memdx.DatatypeFlag(resp.Datatype), resp.Value)
			if err != nil {
				return nil, err
			}

			return &GetResult{
				Value:    value,
				Flags:    resp.Flags,
				Datatype: datatype,
				Cas:      resp.Cas,
			}, nil
		})
}

type GetReplicaOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
	ReplicaIdx     uint32
}

type GetReplicaResult struct {
	Value    []byte
	Flags    uint32
	Datatype memdx.DatatypeFlag
	Cas      uint64
}

func (cc *CrudComponent) GetReplica(ctx context.Context, opts *GetReplicaOptions) (*GetReplicaResult, error) {
	fn := func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*GetReplicaResult, error) {
		resp, err := client.GetReplica(ctx, &memdx.GetReplicaRequest{
			CollectionID: collectionID,
			Key:          opts.Key,
			VbucketID:    vbID,
		})
		if err != nil {
			return nil, err
		}

		value, datatype, err := cc.compression.Decompress(memdx.DatatypeFlag(resp.Datatype), resp.Value)
		if err != nil {
			return nil, err
		}

		return &GetReplicaResult{
			Value:    value,
			Flags:    resp.Flags,
			Datatype: datatype,
			Cas:      resp.Cas,
		}, nil
	}

	return OrchestrateMemdRetries(
		ctx, cc.retries,
		func() (*GetReplicaResult, error) {
			return OrchestrateMemdCollectionID(
				ctx, cc.collections, opts.ScopeName, opts.CollectionName,
				func(collectionID uint32, manifestID uint64) (*GetReplicaResult, error) {
					return OrchestrateMemdRouting(ctx, cc.vbs, cc.cfgmanager, opts.Key, opts.ReplicaIdx, func(endpoint string, vbID uint16) (*GetReplicaResult, error) {
						return OrchestrateMemdClient(ctx, cc.connManager, endpoint, func(client KvClient) (*GetReplicaResult, error) {
							return fn(collectionID, manifestID, endpoint, vbID, client)
						})
					})
				})
		})
}

type UpsertOptions struct {
	Key             []byte
	ScopeName       string
	CollectionName  string
	Value           []byte
	Flags           uint32
	Datatype        memdx.DatatypeFlag
	Expiry          uint32
	PreserveExpiry  bool
	Cas             uint64
	DurabilityLevel memdx.DurabilityLevel
	OnBehalfOf      string
}

type UpsertResult struct {
	Cas           uint64
	MutationToken MutationToken
}

func (cc *CrudComponent) Upsert(ctx context.Context, opts *UpsertOptions) (*UpsertResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*UpsertResult, error) {
			value, datatype, err := cc.compression.Compress(client.HasFeature(memdx.HelloFeatureSnappy), opts.Datatype, opts.Value)
			if err != nil {
				return nil, err
			}

			resp, err := client.Set(ctx, &memdx.SetRequest{
				CollectionID:    collectionID,
				Key:             opts.Key,
				VbucketID:       vbID,
				Value:           value,
				Flags:           opts.Flags,
				Datatype:        uint8(datatype),
				Expiry:          opts.Expiry,
				PreserveExpiry:  opts.PreserveExpiry,
				Cas:             opts.Cas,
				DurabilityLevel: opts.DurabilityLevel,
				OnBehalfOf:      opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &UpsertResult{
				Cas: resp.Cas,
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}

type DeleteOptions struct {
	Key             []byte
	ScopeName       string
	CollectionName  string
	Cas             uint64
	DurabilityLevel memdx.DurabilityLevel
	OnBehalfOf      string
}

type DeleteResult struct {
	Cas           uint64
	MutationToken MutationToken
}

func (cc *CrudComponent) Delete(ctx context.Context, opts *DeleteOptions) (*DeleteResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*DeleteResult, error) {
			resp, err := client.Delete(ctx, &memdx.DeleteRequest{
				CollectionID:    collectionID,
				Key:             opts.Key,
				VbucketID:       vbID,
				Cas:             opts.Cas,
				DurabilityLevel: opts.DurabilityLevel,
				OnBehalfOf:      opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &DeleteResult{
				Cas: resp.Cas,
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}

type GetAndTouchOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
	Expiry         uint32
	OnBehalfOf     string
}

type GetAndTouchResult struct {
	Value    []byte
	Flags    uint32
	Datatype memdx.DatatypeFlag
	Cas      uint64
}

func (cc *CrudComponent) GetAndTouch(ctx context.Context, opts *GetAndTouchOptions) (*GetAndTouchResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*GetAndTouchResult, error) {
			resp, err := client.GetAndTouch(ctx, &memdx.GetAndTouchRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				Expiry:       opts.Expiry,
				OnBehalfOf:   opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			value, datatype, err := cc.compression.Decompress(memdx.DatatypeFlag(resp.Datatype), resp.Value)
			if err != nil {
				return nil, err
			}

			return &GetAndTouchResult{
				Value:    value,
				Flags:    resp.Flags,
				Datatype: datatype,
				Cas:      resp.Cas,
			}, nil
		})
}

type GetRandomOptions struct {
	ScopeName      string
	CollectionName string
	OnBehalfOf     string
}

type GetRandomResult struct {
	Key      []byte
	Value    []byte
	Flags    uint32
	Datatype memdx.DatatypeFlag
	Cas      uint64
}

func (cc *CrudComponent) GetRandom(ctx context.Context, opts *GetRandomOptions) (*GetRandomResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, nil,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*GetRandomResult, error) {
			resp, err := client.GetRandom(ctx, &memdx.GetRandomRequest{
				CollectionID: collectionID,
				OnBehalfOf:   opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			value, datatype, err := cc.compression.Decompress(memdx.DatatypeFlag(resp.Datatype), resp.Value)
			if err != nil {
				return nil, err
			}

			return &GetRandomResult{
				Value:    value,
				Flags:    resp.Flags,
				Datatype: datatype,
				Cas:      resp.Cas,
				Key:      resp.Key,
			}, nil
		})
}

type UnlockOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
	Cas            uint64
	OnBehalfOf     string
}

type UnlockResult struct {
	MutationToken MutationToken
}

func (cc *CrudComponent) Unlock(ctx context.Context, opts *UnlockOptions) (*UnlockResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*UnlockResult, error) {
			resp, err := client.Unlock(ctx, &memdx.UnlockRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				OnBehalfOf:   opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &UnlockResult{
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}

type TouchOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
	Expiry         uint32
	OnBehalfOf     string
}

type TouchResult struct {
	Cas uint64
}

func (cc *CrudComponent) Touch(ctx context.Context, opts *TouchOptions) (*TouchResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*TouchResult, error) {
			resp, err := client.Touch(ctx, &memdx.TouchRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				Expiry:       opts.Expiry,
				OnBehalfOf:   opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &TouchResult{
				Cas: resp.Cas,
			}, nil
		})
}

type GetAndLockOptions struct {
	Key            []byte
	LockTime       uint32
	CollectionName string
	ScopeName      string
	CollectionID   uint32
}

type GetAndLockResult struct {
	Value    []byte
	Flags    uint32
	Datatype memdx.DatatypeFlag
	Cas      uint64
}

func (cc *CrudComponent) GetAndLock(ctx context.Context, opts *GetAndLockOptions) (*GetAndLockResult, error) {
	return OrchestrateSimpleCrud(ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager, opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*GetAndLockResult, error) {
			resp, err := client.GetAndLock(ctx, &memdx.GetAndLockRequest{
				CollectionID: collectionID,
				LockTime:     opts.LockTime,
				Key:          opts.Key,
				VbucketID:    vbID,
			})
			if err != nil {
				return nil, err
			}

			value, datatype, err := cc.compression.Decompress(memdx.DatatypeFlag(resp.Datatype), resp.Value)
			if err != nil {
				return nil, err
			}

			return &GetAndLockResult{
				Cas:      resp.Cas,
				Value:    value,
				Datatype: datatype,
				Flags:    resp.Flags,
			}, nil
		})
}

type AddOptions struct {
	Key             []byte
	ScopeName       string
	CollectionName  string
	Flags           uint32
	Value           []byte
	Datatype        memdx.DatatypeFlag
	Expiry          uint32
	DurabilityLevel memdx.DurabilityLevel
	OnBehalfOf      string
}

type AddResult struct {
	Cas           uint64
	MutationToken MutationToken
}

func (cc *CrudComponent) Add(ctx context.Context, opts *AddOptions) (*AddResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*AddResult, error) {
			value, datatype, err := cc.compression.Compress(client.HasFeature(memdx.HelloFeatureSnappy), opts.Datatype, opts.Value)
			if err != nil {
				return nil, err
			}

			resp, err := client.Add(ctx, &memdx.AddRequest{
				CollectionID:    collectionID,
				Key:             opts.Key,
				VbucketID:       vbID,
				Flags:           opts.Flags,
				Value:           value,
				Datatype:        uint8(datatype),
				Expiry:          opts.Expiry,
				DurabilityLevel: opts.DurabilityLevel,
				OnBehalfOf:      opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &AddResult{
				Cas: resp.Cas,
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}

type ReplaceOptions struct {
	Key             []byte
	ScopeName       string
	CollectionName  string
	Flags           uint32
	Value           []byte
	Datatype        memdx.DatatypeFlag
	Expiry          uint32
	PreserveExpiry  bool
	Cas             uint64
	DurabilityLevel memdx.DurabilityLevel
	OnBehalfOf      string
}

type ReplaceResult struct {
	Cas           uint64
	MutationToken MutationToken
}

func (cc *CrudComponent) Replace(ctx context.Context, opts *ReplaceOptions) (*ReplaceResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*ReplaceResult, error) {
			value, datatype, err := cc.compression.Compress(client.HasFeature(memdx.HelloFeatureSnappy), opts.Datatype, opts.Value)
			if err != nil {
				return nil, err
			}

			resp, err := client.Replace(ctx, &memdx.ReplaceRequest{
				CollectionID:    collectionID,
				Key:             opts.Key,
				VbucketID:       vbID,
				Flags:           opts.Flags,
				Value:           value,
				Datatype:        uint8(datatype),
				Expiry:          opts.Expiry,
				PreserveExpiry:  opts.PreserveExpiry,
				Cas:             opts.Cas,
				DurabilityLevel: opts.DurabilityLevel,
				OnBehalfOf:      opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &ReplaceResult{
				Cas: resp.Cas,
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}

type AppendOptions struct {
	Key             []byte
	ScopeName       string
	CollectionName  string
	Value           []byte
	DurabilityLevel memdx.DurabilityLevel
	OnBehalfOf      string
}

type AppendResult struct {
	Cas           uint64
	MutationToken MutationToken
}

func (cc *CrudComponent) Append(ctx context.Context, opts *AppendOptions) (*AppendResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*AppendResult, error) {
			value, datatype, err := cc.compression.Compress(client.HasFeature(memdx.HelloFeatureSnappy), 0, opts.Value)
			if err != nil {
				return nil, err
			}

			resp, err := client.Append(ctx, &memdx.AppendRequest{
				CollectionID:    collectionID,
				Key:             opts.Key,
				VbucketID:       vbID,
				Value:           value,
				Datatype:        uint8(datatype),
				DurabilityLevel: opts.DurabilityLevel,
				OnBehalfOf:      opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &AppendResult{
				Cas: resp.Cas,
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}

type PrependOptions struct {
	Key             []byte
	ScopeName       string
	CollectionName  string
	Value           []byte
	DurabilityLevel memdx.DurabilityLevel
	OnBehalfOf      string
}

type PrependResult struct {
	Cas           uint64
	MutationToken MutationToken
}

func (cc *CrudComponent) Prepend(ctx context.Context, opts *PrependOptions) (*PrependResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*PrependResult, error) {
			value, datatype, err := cc.compression.Compress(client.HasFeature(memdx.HelloFeatureSnappy), 0, opts.Value)
			if err != nil {
				return nil, err
			}

			resp, err := client.Prepend(ctx, &memdx.PrependRequest{
				CollectionID:    collectionID,
				Key:             opts.Key,
				VbucketID:       vbID,
				Value:           value,
				Datatype:        uint8(datatype),
				DurabilityLevel: opts.DurabilityLevel,
				OnBehalfOf:      opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &PrependResult{
				Cas: resp.Cas,
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}

type IncrementOptions struct {
	Key             []byte
	ScopeName       string
	CollectionName  string
	Value           []byte
	Initial         uint64
	Delta           uint64
	Expiry          uint32
	DurabilityLevel memdx.DurabilityLevel
	OnBehalfOf      string
}

type IncrementResult struct {
	Cas           uint64
	Value         uint64
	MutationToken MutationToken
}

func (cc *CrudComponent) Increment(ctx context.Context, opts *IncrementOptions) (*IncrementResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*IncrementResult, error) {
			resp, err := client.Increment(ctx, &memdx.IncrementRequest{
				CollectionID:    collectionID,
				Key:             opts.Key,
				VbucketID:       vbID,
				Initial:         opts.Initial,
				Delta:           opts.Delta,
				Expiry:          opts.Expiry,
				DurabilityLevel: opts.DurabilityLevel,
				OnBehalfOf:      opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &IncrementResult{
				Cas:   resp.Cas,
				Value: resp.Value,
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}

type DecrementOptions struct {
	Key             []byte
	ScopeName       string
	CollectionName  string
	Value           []byte
	Initial         uint64
	Delta           uint64
	Expiry          uint32
	DurabilityLevel memdx.DurabilityLevel
	OnBehalfOf      string
}

type DecrementResult struct {
	Cas           uint64
	Value         uint64
	MutationToken MutationToken
}

func (cc *CrudComponent) Decrement(ctx context.Context, opts *DecrementOptions) (*DecrementResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*DecrementResult, error) {
			resp, err := client.Decrement(ctx, &memdx.DecrementRequest{
				CollectionID:    collectionID,
				Key:             opts.Key,
				VbucketID:       vbID,
				Initial:         opts.Initial,
				Delta:           opts.Delta,
				Expiry:          opts.Expiry,
				DurabilityLevel: opts.DurabilityLevel,
				OnBehalfOf:      opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &DecrementResult{
				Cas:   resp.Cas,
				Value: resp.Value,
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}

type GetMetaOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
	OnBehalfOf     string
}

type GetMetaResult struct {
	Value    []byte
	Flags    uint32
	Cas      uint64
	Expiry   uint32
	SeqNo    uint64
	Datatype memdx.DatatypeFlag
	Deleted  uint32
}

func (cc *CrudComponent) GetMeta(ctx context.Context, opts *GetMetaOptions) (*GetMetaResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*GetMetaResult, error) {
			resp, err := client.GetMeta(ctx, &memdx.GetMetaRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				OnBehalfOf:   opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &GetMetaResult{
				Value:    resp.Value,
				Flags:    resp.Flags,
				Cas:      resp.Cas,
				Expiry:   resp.Expiry,
				SeqNo:    resp.SeqNo,
				Datatype: memdx.DatatypeFlag(resp.Datatype),
				Deleted:  resp.Deleted,
			}, nil
		})
}

type SetMetaOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
	Value          []byte
	Flags          uint32
	Datatype       uint8
	Expiry         uint32
	Extra          []byte
	RevNo          uint64
	Cas            uint64
	Options        uint32
	OnBehalfOf     string
}

type SetMetaResult struct {
	Cas           uint64
	MutationToken MutationToken
}

func (cc *CrudComponent) SetMeta(ctx context.Context, opts *SetMetaOptions) (*SetMetaResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*SetMetaResult, error) {
			resp, err := client.SetMeta(ctx, &memdx.SetMetaRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				Flags:        opts.Flags,
				Value:        opts.Value,
				Datatype:     opts.Datatype,
				Expiry:       opts.Expiry,
				Extra:        opts.Extra,
				RevNo:        opts.RevNo,
				Cas:          opts.Cas,
				Options:      opts.Options,
				OnBehalfOf:   opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &SetMetaResult{
				Cas: resp.Cas,
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}

type DeleteMetaOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
	Flags          uint32
	Expiry         uint32
	Extra          []byte
	RevNo          uint64
	Cas            uint64
	Options        uint32
	OnBehalfOf     string
}

type DeleteMetaResult struct {
	Cas           uint64
	MutationToken MutationToken
}

func (cc *CrudComponent) DeleteMeta(ctx context.Context, opts *DeleteMetaOptions) (*DeleteMetaResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*DeleteMetaResult, error) {
			resp, err := client.DeleteMeta(ctx, &memdx.DeleteMetaRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				Flags:        opts.Flags,
				Expiry:       opts.Expiry,
				Extra:        opts.Extra,
				RevNo:        opts.RevNo,
				Cas:          opts.Cas,
				Options:      opts.Options,
				OnBehalfOf:   opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &DeleteMetaResult{
				Cas: resp.Cas,
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}

type LookupInOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
	Value          []byte
	Flags          uint8
	OnBehalfOf     string
}

type LookupInResult struct {
	Value   []byte
	Deleted bool
	Cas     uint64
}

func (cc *CrudComponent) LookupIn(ctx context.Context, opts *LookupInOptions) (*LookupInResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*LookupInResult, error) {
			resp, err := client.LookupIn(ctx, &memdx.LookupInRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				Flags:        opts.Flags,
				Value:        opts.Value,
				OnBehalfOf:   opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &LookupInResult{
				Value:   resp.Value,
				Deleted: resp.Deleted,
				Cas:     resp.Cas,
			}, nil
		})
}

type MutateInOptions struct {
	Key             []byte
	ScopeName       string
	CollectionName  string
	Value           []byte
	Flags           uint32
	Expiry          uint32
	PreserveExpiry  bool
	Cas             uint64
	DurabilityLevel memdx.DurabilityLevel
	OnBehalfOf      string
}

type MutateInResult struct {
	Cas           uint64
	Value         []byte
	MutationToken MutationToken
}

func (cc *CrudComponent) MutateIn(ctx context.Context, opts *MutateInOptions) (*MutateInResult, error) {
	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.cfgmanager, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*MutateInResult, error) {
			resp, err := client.MutateIn(ctx, &memdx.MutateInRequest{
				CollectionID:    collectionID,
				Key:             opts.Key,
				VbucketID:       vbID,
				Flags:           opts.Flags,
				Value:           opts.Value,
				Expiry:          opts.Expiry,
				PreserveExpiry:  opts.PreserveExpiry,
				Cas:             opts.Cas,
				DurabilityLevel: opts.DurabilityLevel,
				OnBehalfOf:      opts.OnBehalfOf,
			})
			if err != nil {
				return nil, err
			}

			return &MutateInResult{
				Cas:   resp.Cas,
				Value: resp.Value,
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}
