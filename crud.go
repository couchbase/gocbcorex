package gocbcorex

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/helpers/subdocpath"
	"github.com/couchbase/gocbcorex/helpers/subdocprojection"
	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type CrudComponent struct {
	logger      *zap.Logger
	collections CollectionResolver
	nmvHandler  NotMyVbucketConfigHandler
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
	ch NotMyVbucketConfigHandler,
	nkcp KvClientManager,
	scopeName, collectionName string,
	key []byte,
	fn func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (RespT, error),
) (RespT, error) {
	return OrchestrateRetries(
		ctx, rs,
		func() (RespT, error) {
			return OrchestrateMemdCollectionID(
				ctx, cr, scopeName, collectionName,
				func(collectionID uint32, manifestID uint64) (RespT, error) {
					return OrchestrateMemdRouting(ctx, vb, ch, key, 0, func(endpoint string, vbID uint16) (RespT, error) {
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
	ctx, span := tracer.Start(ctx, "Get")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*GetResult, error) {
			resp, err := client.Get(ctx, &memdx.GetRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	OnBehalfOf     string
}

type GetReplicaResult struct {
	Value    []byte
	Flags    uint32
	Datatype memdx.DatatypeFlag
	Cas      uint64
}

func (cc *CrudComponent) GetReplica(ctx context.Context, opts *GetReplicaOptions) (*GetReplicaResult, error) {
	ctx, span := tracer.Start(ctx, "GetReplica")
	defer span.End()

	fn := func(collectionID uint32, vbID uint16, client KvClient) (*GetReplicaResult, error) {
		resp, err := client.GetReplica(ctx, &memdx.GetReplicaRequest{
			CollectionID: collectionID,
			Key:          opts.Key,
			VbucketID:    vbID,
			CrudRequestMeta: memdx.CrudRequestMeta{
				OnBehalfOf: opts.OnBehalfOf,
			},
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

	vbServerIdx := 1 + opts.ReplicaIdx
	return OrchestrateRetries(
		ctx, cc.retries,
		func() (*GetReplicaResult, error) {
			return OrchestrateMemdCollectionID(
				ctx, cc.collections, opts.ScopeName, opts.CollectionName,
				func(collectionID uint32, manifestID uint64) (*GetReplicaResult, error) {
					return OrchestrateMemdRouting(ctx, cc.vbs, cc.nmvHandler, opts.Key, vbServerIdx, func(endpoint string, vbID uint16) (*GetReplicaResult, error) {
						return OrchestrateMemdClient(ctx, cc.connManager, endpoint, func(client KvClient) (*GetReplicaResult, error) {
							return fn(collectionID, vbID, client)
						})
					})
				})
		})
}

type GetAllReplicasOptions struct {
	Key            []byte
	BucketName     string
	ScopeName      string
	CollectionName string
	OnBehalfOf     string
}

type GetAllReplicasResult struct {
	Value     []byte
	Flags     uint32
	Datatype  memdx.DatatypeFlag
	Cas       uint64
	IsReplica bool
}

type ReplicaStreamEntry struct {
	Err error
	Res *GetAllReplicasResult
}

type ReplicaStream struct {
	OutCh chan *ReplicaStreamEntry
}

func (s ReplicaStream) Next() (*GetAllReplicasResult, error) {
	res := <-s.OutCh
	if res == nil {
		return nil, nil
	}
	return res.Res, res.Err
}

type GetAllReplicaStream interface {
	Next() (*GetAllReplicasResult, error)
}

func (cc *CrudComponent) GetAllReplicas(ctx context.Context, opts *GetAllReplicasOptions) (GetAllReplicaStream, error) {
	maxReplicas := 3
	result := ReplicaStream{
		OutCh: make(chan *ReplicaStreamEntry, maxReplicas+1),
	}

	getFn := func(collectionID uint32, vbID uint16, client KvClient) (*GetAllReplicasResult, error) {
		resp, err := client.Get(ctx, &memdx.GetRequest{
			CollectionID: collectionID,
			Key:          opts.Key,
			VbucketID:    vbID,
			CrudRequestMeta: memdx.CrudRequestMeta{
				OnBehalfOf: opts.OnBehalfOf,
			},
		})
		if err != nil {
			return nil, err
		}

		value, datatype, err := cc.compression.Decompress(memdx.DatatypeFlag(resp.Datatype), resp.Value)
		if err != nil {
			return nil, err
		}

		return &GetAllReplicasResult{
			Value:     value,
			Flags:     resp.Flags,
			Datatype:  datatype,
			Cas:       resp.Cas,
			IsReplica: false,
		}, nil
	}

	getReplicaFn := func(collectionID uint32, vbID uint16, client KvClient) (*GetAllReplicasResult, error) {
		resp, err := client.GetReplica(ctx, &memdx.GetReplicaRequest{
			CollectionID: collectionID,
			Key:          opts.Key,
			VbucketID:    vbID,
			CrudRequestMeta: memdx.CrudRequestMeta{
				OnBehalfOf: opts.OnBehalfOf,
			},
		})
		if err != nil {
			return nil, err
		}

		value, datatype, err := cc.compression.Decompress(memdx.DatatypeFlag(resp.Datatype), resp.Value)
		if err != nil {
			return nil, err
		}

		return &GetAllReplicasResult{
			Value:     value,
			Flags:     resp.Flags,
			Datatype:  datatype,
			Cas:       resp.Cas,
			IsReplica: true,
		}, nil
	}

	var endpoints []string
	var mu sync.Mutex
	var sendLock sync.Mutex
	var returnedResults uint32
	var numReplicas atomic.Uint32

	initialReplicas, err := cc.vbs.NumReplicas()
	if err != nil {
		return nil, err
	}
	numReplicas.Store(uint32(initialReplicas))

	// This retry orchestrator handles request level retryable errors, errors which impact every replica request,
	// e.g the collection ID not yet being consistent
	_, err = OrchestrateRetries(ctx, cc.retries, func() (any, error) {
		return OrchestrateMemdCollectionID(ctx, cc.collections, opts.ScopeName, opts.CollectionName,
			func(collectionID uint32, manifestID uint64) (any, error) {

				// We are past the point of no return. From here on the request cannot error, e.g a nil error will always be
				// returned from GetAllReplicas, however individual replica reads can push an error to the channel.
				for i := uint32(0); i <= numReplicas.Load(); i++ {
					go func(replicaID uint32) {
						for {
							res, err := OrchestrateReplicaRead(ctx, cc.vbs, cc.nmvHandler, cc.connManager, opts.Key, replicaID,
								func(ep string, vbID uint16, client KvClient) (*GetAllReplicasResult, error) {
									mu.Lock()
									if uint32(len(endpoints)) == numReplicas.Load()+1 {
										mu.Unlock()
										return nil, nil
									}

									// The replicaID is being routed to a node from which we have already received a result
									// the appropriate error returned and the retry manager will retry
									if slices.Contains(endpoints, ep) {
										mu.Unlock()
										return nil, ErrRepeatedReplicaRead
									}
									mu.Unlock()

									var res *GetAllReplicasResult
									var err error

									if replicaID == 0 {
										res, err = getFn(collectionID, vbID, client)
									} else {
										res, err = getReplicaFn(collectionID, vbID, client)
									}

									mu.Lock()
									// If a rebalance occured while we were executing the function to get the doc
									// a thread with a different replicaID may have been routed to the same endpoint,
									// fetched the replica and added to the endpoints slice, so we repeat the check
									if err == nil && !slices.Contains(endpoints, ep) {
										endpoints = append(endpoints, ep)
										mu.Unlock()
										return res, err
									}
									mu.Unlock()
									return nil, err
								})

							// If we read a replica from a node that we have already read from we wait for a short time
							// before trying this replica again. Since there may be a rebalance where the replicaId is moved to
							// a new node
							if errors.Is(err, ErrRepeatedReplicaRead) {
								time.Sleep(10 * time.Millisecond)
								continue
							}

							// If we get invalid replica then the number of replicas has been reduced since we started
							// got numReplicas. Therefore we decrease numReplicas and kill this thread
							if errors.Is(err, ErrInvalidReplica) {
								numReplicas.Dec()
								break
							}

							sendLock.Lock()
							// Check another thread hasn't sent a final result so we don't try and send on a closed channel
							if returnedResults == numReplicas.Load()+1 {
								sendLock.Unlock()
								break
							}

							returnedResults++
							result.OutCh <- &ReplicaStreamEntry{
								Err: err,
								Res: res,
							}

							if returnedResults == numReplicas.Load()+1 {
								close(result.OutCh)
								sendLock.Unlock()
								break
							}
							sendLock.Unlock()

							// Although we have returned a result from this thread we wait then continue running in case
							// there is a rebalance which causes the replicaID of this thread to be routed to a different node.
							time.Sleep(10 * time.Millisecond)
						}
					}(i)
				}
				// Always return nil, nil here because any errors encountered from OrchestrateRetries are regarded
				// as Replica specific, to be streamed back to the user, not propogated back to OrchestrateMemdCollectionID
				return nil, nil
			})
	})

	// A terminal request error so can close the channel since no replicas are being fetched
	if err != nil {
		close(result.OutCh)
	}

	return result, err
}

func OrchestrateReplicaRead(
	ctx context.Context,
	vb VbucketRouter,
	ch NotMyVbucketConfigHandler,
	nkcp KvClientManager,
	key []byte,
	replica uint32,
	fn func(ep string, vbID uint16, client KvClient) (*GetAllReplicasResult, error),
) (*GetAllReplicasResult, error) {
	rs := NewRetryManagerDefault()
	return OrchestrateRetries(ctx, rs, func() (*GetAllReplicasResult, error) {
		return OrchestrateMemdRouting(ctx, vb, ch, key, replica, func(endpoint string, vbID uint16) (*GetAllReplicasResult, error) {
			return OrchestrateMemdClient(ctx, nkcp, endpoint, func(client KvClient) (*GetAllReplicasResult, error) {
				return fn(endpoint, vbID, client)
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
	ctx, span := tracer.Start(ctx, "Upsert")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
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
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	ctx, span := tracer.Start(ctx, "Delete")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*DeleteResult, error) {
			resp, err := client.Delete(ctx, &memdx.DeleteRequest{
				CollectionID:    collectionID,
				Key:             opts.Key,
				VbucketID:       vbID,
				Cas:             opts.Cas,
				DurabilityLevel: opts.DurabilityLevel,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	ctx, span := tracer.Start(ctx, "GetAndTouch")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*GetAndTouchResult, error) {
			resp, err := client.GetAndTouch(ctx, &memdx.GetAndTouchRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				Expiry:       opts.Expiry,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	ctx, span := tracer.Start(ctx, "GetRandom")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
		opts.ScopeName, opts.CollectionName, nil,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*GetRandomResult, error) {
			resp, err := client.GetRandom(ctx, &memdx.GetRandomRequest{
				CollectionID: collectionID,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	ctx, span := tracer.Start(ctx, "Unlock")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*UnlockResult, error) {
			resp, err := client.Unlock(ctx, &memdx.UnlockRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				Cas:          opts.Cas,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	ctx, span := tracer.Start(ctx, "Touch")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*TouchResult, error) {
			resp, err := client.Touch(ctx, &memdx.TouchRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				Expiry:       opts.Expiry,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	OnBehalfOf     string
}

type GetAndLockResult struct {
	Value    []byte
	Flags    uint32
	Datatype memdx.DatatypeFlag
	Cas      uint64
}

func (cc *CrudComponent) GetAndLock(ctx context.Context, opts *GetAndLockOptions) (*GetAndLockResult, error) {
	ctx, span := tracer.Start(ctx, "GetAndLock")
	defer span.End()

	return OrchestrateSimpleCrud(ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager, opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*GetAndLockResult, error) {
			resp, err := client.GetAndLock(ctx, &memdx.GetAndLockRequest{
				CollectionID: collectionID,
				LockTime:     opts.LockTime,
				Key:          opts.Key,
				VbucketID:    vbID,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	ctx, span := tracer.Start(ctx, "Add")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
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
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	ctx, span := tracer.Start(ctx, "Replace")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
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
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	Cas             uint64
	DurabilityLevel memdx.DurabilityLevel
	OnBehalfOf      string
}

type AppendResult struct {
	Cas           uint64
	MutationToken MutationToken
}

func (cc *CrudComponent) Append(ctx context.Context, opts *AppendOptions) (*AppendResult, error) {
	ctx, span := tracer.Start(ctx, "Append")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
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
				Cas:             opts.Cas,
				DurabilityLevel: opts.DurabilityLevel,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	Cas             uint64
	DurabilityLevel memdx.DurabilityLevel
	OnBehalfOf      string
}

type PrependResult struct {
	Cas           uint64
	MutationToken MutationToken
}

func (cc *CrudComponent) Prepend(ctx context.Context, opts *PrependOptions) (*PrependResult, error) {
	ctx, span := tracer.Start(ctx, "Prepend")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
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
				Cas:             opts.Cas,
				DurabilityLevel: opts.DurabilityLevel,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	ctx, span := tracer.Start(ctx, "Increment")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
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
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	ctx, span := tracer.Start(ctx, "Decrement")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
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
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	Deleted  bool
}

func (cc *CrudComponent) GetMeta(ctx context.Context, opts *GetMetaOptions) (*GetMetaResult, error) {
	ctx, span := tracer.Start(ctx, "GetMeta")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*GetMetaResult, error) {
			resp, err := client.GetMeta(ctx, &memdx.GetMetaRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	ctx, span := tracer.Start(ctx, "SetMeta")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
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
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	ctx, span := tracer.Start(ctx, "DeleteMeta")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
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
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
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
	Ops            []memdx.LookupInOp
	Flags          memdx.SubdocDocFlag
	OnBehalfOf     string
}

type LookupInResult struct {
	Ops          []memdx.SubDocResult
	DocIsDeleted bool
	Cas          uint64
}

func (cc *CrudComponent) LookupIn(ctx context.Context, opts *LookupInOptions) (*LookupInResult, error) {
	ctx, span := tracer.Start(ctx, "LookupIn")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*LookupInResult, error) {
			resp, err := client.LookupIn(ctx, &memdx.LookupInRequest{
				CollectionID: collectionID,
				Key:          opts.Key,
				VbucketID:    vbID,
				Flags:        opts.Flags,
				Ops:          opts.Ops,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
			})
			if err != nil {
				return nil, err
			}

			return &LookupInResult{
				Ops:          resp.Ops,
				DocIsDeleted: resp.DocIsDeleted,
				Cas:          resp.Cas,
			}, nil
		})
}

type MutateInOptions struct {
	Key             []byte
	ScopeName       string
	CollectionName  string
	Ops             []memdx.MutateInOp
	Flags           memdx.SubdocDocFlag
	Expiry          uint32
	PreserveExpiry  bool
	Cas             uint64
	DurabilityLevel memdx.DurabilityLevel
	OnBehalfOf      string
}

type MutateInResult struct {
	Cas           uint64
	Ops           []memdx.SubDocResult
	MutationToken MutationToken
}

func (cc *CrudComponent) MutateIn(ctx context.Context, opts *MutateInOptions) (*MutateInResult, error) {
	ctx, span := tracer.Start(ctx, "MutateIn")
	defer span.End()

	return OrchestrateSimpleCrud(
		ctx, cc.retries, cc.collections, cc.vbs, cc.nmvHandler, cc.connManager,
		opts.ScopeName, opts.CollectionName, opts.Key,
		func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (*MutateInResult, error) {
			resp, err := client.MutateIn(ctx, &memdx.MutateInRequest{
				CollectionID:    collectionID,
				Key:             opts.Key,
				VbucketID:       vbID,
				Flags:           opts.Flags,
				Ops:             opts.Ops,
				Expiry:          opts.Expiry,
				PreserveExpiry:  opts.PreserveExpiry,
				Cas:             opts.Cas,
				DurabilityLevel: opts.DurabilityLevel,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
			})
			if err != nil {
				return nil, err
			}

			return &MutateInResult{
				Cas: resp.Cas,
				Ops: resp.Ops,
				MutationToken: MutationToken{
					VbID:   vbID,
					VbUuid: resp.MutationToken.VbUuid,
					SeqNo:  resp.MutationToken.SeqNo,
				},
			}, nil
		})
}

type GetOrLookupOptions struct {
	Key            []byte
	ScopeName      string
	CollectionName string
	Project        []string
	WithExpiry     bool
	WithFlags      bool
	OnBehalfOf     string
}

type GetOrLookupResult struct {
	Value    []byte
	Flags    uint32
	Datatype memdx.DatatypeFlag
	Cas      uint64
	Expiry   uint32
}

func (cc *CrudComponent) GetOrLookup(ctx context.Context, opts *GetOrLookupOptions) (*GetOrLookupResult, error) {
	if len(opts.Project) == 0 && !opts.WithExpiry {
		resp, err := cc.Get(ctx, &GetOptions{
			Key:            opts.Key,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			OnBehalfOf:     opts.OnBehalfOf,
		})
		if err != nil {
			return nil, err
		}

		// if the user does not want flags, we remove them so they do not
		// accidentally get used when it wasn't expected to be there.
		flags := resp.Flags
		if !opts.WithFlags {
			flags = 0
		}

		return &GetOrLookupResult{
			Value:    resp.Value,
			Flags:    flags,
			Datatype: resp.Datatype,
			Cas:      resp.Cas,
			Expiry:   0,
		}, nil
	}

	var executeGet func(forceFullDoc bool) (*GetOrLookupResult, error)
	executeGet = func(forceFullDoc bool) (*GetOrLookupResult, error) {
		var opOpts LookupInOptions
		opOpts.OnBehalfOf = opts.OnBehalfOf
		opOpts.ScopeName = opts.ScopeName
		opOpts.CollectionName = opts.CollectionName
		opOpts.Key = opts.Key

		withFlags := opts.WithFlags
		withExpiry := opts.WithExpiry

		flagsOffset := -1
		if withFlags {
			opOpts.Ops = append(opOpts.Ops, memdx.LookupInOp{
				Op:    memdx.LookupInOpTypeGet,
				Flags: memdx.SubdocOpFlagXattrPath,
				Path:  []byte("$document.flags"),
			})
			flagsOffset = len(opOpts.Ops) - 1
		}

		expiryOffset := -1
		if withExpiry {
			opOpts.Ops = append(opOpts.Ops, memdx.LookupInOp{
				Op:    memdx.LookupInOpTypeGet,
				Flags: memdx.SubdocOpFlagXattrPath,
				Path:  []byte("$document.exptime"),
			})
			expiryOffset = len(opOpts.Ops) - 1
		}

		userProjectOffset := len(opOpts.Ops)
		maxUserProjections := 16 - userProjectOffset

		isFullDocFetch := false
		if len(opts.Project) > 0 && len(opts.Project) < maxUserProjections && !forceFullDoc {
			for _, projectPath := range opts.Project {
				opOpts.Ops = append(opOpts.Ops, memdx.LookupInOp{
					Op:    memdx.LookupInOpTypeGet,
					Flags: memdx.SubdocOpFlagNone,
					Path:  []byte(projectPath),
				})
			}

			isFullDocFetch = false
		} else {
			opOpts.Ops = append(opOpts.Ops, memdx.LookupInOp{
				Op:    memdx.LookupInOpTypeGetDoc,
				Flags: memdx.SubdocOpFlagNone,
				Path:  nil,
			})

			isFullDocFetch = true
		}

		result, err := cc.LookupIn(ctx, &opOpts)
		if err != nil {
			return nil, err
		}

		var flags uint32
		if flagsOffset >= 0 {
			parsedFlags, err := strconv.ParseUint(string(result.Ops[flagsOffset].Value), 10, 64)
			if err != nil {
				return nil, err
			}

			flags = uint32(parsedFlags)
		}

		var expiryTime uint32
		if expiryOffset >= 0 {
			parsedExpiryTime, err := strconv.ParseInt(string(result.Ops[expiryOffset].Value), 10, 64)
			if err != nil {
				return nil, err
			}

			expiryTime = uint32(parsedExpiryTime)
		}

		if len(opts.Project) > 0 {
			var writer subdocprojection.Projector

			if isFullDocFetch {
				docValue := result.Ops[userProjectOffset].Value

				var reader subdocprojection.Projector

				err := reader.Init(docValue)
				if err != nil {
					return nil, err
				}

				for _, path := range opts.Project {
					parsedPath, err := subdocpath.Parse(path)
					if err != nil {
						return nil, &PathProjectionError{
							Path:  path,
							Cause: err,
						}
					}

					pathValue, err := reader.Get(parsedPath)
					if err != nil {
						return nil, &PathProjectionError{
							Path:  path,
							Cause: err,
						}
					}

					err = writer.Set(parsedPath, pathValue)
					if err != nil {
						return nil, &PathProjectionError{
							Path:  path,
							Cause: err,
						}
					}
				}
			} else {
				for pathIdx, path := range opts.Project {
					op := result.Ops[userProjectOffset+pathIdx]

					if op.Err != nil {
						if errors.Is(op.Err, memdx.ErrSubDocDocTooDeep) {
							cc.logger.Debug("falling back to fulldoc projection due to ErrSubDocDocTooDeep")
							return executeGet(true)
						} else if errors.Is(op.Err, memdx.ErrSubDocNotJSON) {
							// this is actually a document error, not a path error
							return nil, op.Err
						} else if errors.Is(op.Err, memdx.ErrSubDocPathNotFound) {
							// path not founds are skipped and not included in the
							// output document rather than triggering errors.
							continue
						} else if errors.Is(op.Err, memdx.ErrSubDocPathInvalid) {
							return nil, &PathProjectionError{
								Path:  path,
								Cause: op.Err,
							}
						} else if errors.Is(op.Err, memdx.ErrSubDocPathMismatch) {
							return nil, &PathProjectionError{
								Path:  path,
								Cause: op.Err,
							}
						} else if errors.Is(op.Err, memdx.ErrSubDocPathTooBig) {
							cc.logger.Debug("falling back to fulldoc projection due to ErrSubDocPathTooBig")
							return executeGet(true)
						}

						cc.logger.Debug("falling back to fulldoc projection due to unexpected op error", zap.Error(op.Err))
						return executeGet(true)
					}

					parsedPath, err := subdocpath.Parse(path)
					if err != nil {
						return nil, &PathProjectionError{
							Path:  path,
							Cause: err,
						}
					}

					err = writer.Set(parsedPath, op.Value)
					if err != nil {
						return nil, &PathProjectionError{
							Path:  path,
							Cause: err,
						}
					}
				}
			}

			projectedDocValue, err := writer.Build()
			if err != nil {
				return nil, err
			}

			return &GetOrLookupResult{
				Value:    projectedDocValue,
				Flags:    flags,
				Datatype: 0,
				Cas:      result.Cas,
				Expiry:   expiryTime,
			}, nil
		}

		docValue := result.Ops[userProjectOffset].Value

		return &GetOrLookupResult{
			Value:    docValue,
			Flags:    flags,
			Datatype: 0,
			Cas:      result.Cas,
			Expiry:   expiryTime,
		}, nil
	}

	return executeGet(false)
}
