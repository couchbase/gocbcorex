package gocbcorex

import (
	"context"

	"github.com/couchbase/gocbcorex/cbmgmtx"
)

func (agent *Agent) Upsert(ctx context.Context, opts *UpsertOptions) (*UpsertResult, error) {
	return agent.crud.Upsert(ctx, opts)
}

func (agent *Agent) Get(ctx context.Context, opts *GetOptions) (*GetResult, error) {
	return agent.crud.Get(ctx, opts)
}

func (agent *Agent) GetReplica(ctx context.Context, opts *GetReplicaOptions) (*GetReplicaResult, error) {
	return agent.crud.GetReplica(ctx, opts)
}

func (agent *Agent) Delete(ctx context.Context, opts *DeleteOptions) (*DeleteResult, error) {
	return agent.crud.Delete(ctx, opts)
}

func (agent *Agent) GetAndLock(ctx context.Context, opts *GetAndLockOptions) (*GetAndLockResult, error) {
	return agent.crud.GetAndLock(ctx, opts)
}

func (agent *Agent) GetAndTouch(ctx context.Context, opts *GetAndTouchOptions) (*GetAndTouchResult, error) {
	return agent.crud.GetAndTouch(ctx, opts)
}

func (agent *Agent) GetRandom(ctx context.Context, opts *GetRandomOptions) (*GetRandomResult, error) {
	return agent.crud.GetRandom(ctx, opts)
}

func (agent *Agent) Unlock(ctx context.Context, opts *UnlockOptions) (*UnlockResult, error) {
	return agent.crud.Unlock(ctx, opts)
}

func (agent *Agent) Touch(ctx context.Context, opts *TouchOptions) (*TouchResult, error) {
	return agent.crud.Touch(ctx, opts)
}

func (agent *Agent) Add(ctx context.Context, opts *AddOptions) (*AddResult, error) {
	return agent.crud.Add(ctx, opts)
}

func (agent *Agent) Replace(ctx context.Context, opts *ReplaceOptions) (*ReplaceResult, error) {
	return agent.crud.Replace(ctx, opts)
}

func (agent *Agent) Append(ctx context.Context, opts *AppendOptions) (*AppendResult, error) {
	return agent.crud.Append(ctx, opts)
}

func (agent *Agent) Prepend(ctx context.Context, opts *PrependOptions) (*PrependResult, error) {
	return agent.crud.Prepend(ctx, opts)
}

func (agent *Agent) Increment(ctx context.Context, opts *IncrementOptions) (*IncrementResult, error) {
	return agent.crud.Increment(ctx, opts)
}

func (agent *Agent) Decrement(ctx context.Context, opts *DecrementOptions) (*DecrementResult, error) {
	return agent.crud.Decrement(ctx, opts)
}

func (agent *Agent) GetMeta(ctx context.Context, opts *GetMetaOptions) (*GetMetaResult, error) {
	return agent.crud.GetMeta(ctx, opts)
}

func (agent *Agent) SetMeta(ctx context.Context, opts *SetMetaOptions) (*SetMetaResult, error) {
	return agent.crud.SetMeta(ctx, opts)
}

func (agent *Agent) DeleteMeta(ctx context.Context, opts *DeleteMetaOptions) (*DeleteMetaResult, error) {
	return agent.crud.DeleteMeta(ctx, opts)
}

func (agent *Agent) LookupIn(ctx context.Context, opts *LookupInOptions) (*LookupInResult, error) {
	return agent.crud.LookupIn(ctx, opts)
}

func (agent *Agent) MutateIn(ctx context.Context, opts *MutateInOptions) (*MutateInResult, error) {
	return agent.crud.MutateIn(ctx, opts)
}

func (agent *Agent) Query(ctx context.Context, opts *QueryOptions) (QueryResultStream, error) {
	return agent.query.Query(ctx, opts)
}

func (agent *Agent) PreparedQuery(ctx context.Context, opts *QueryOptions) (QueryResultStream, error) {
	return agent.query.PreparedQuery(ctx, opts)
}

func (agent *Agent) GetCollectionManifest(ctx context.Context, opts *cbmgmtx.GetCollectionManifestOptions) (*cbmgmtx.CollectionManifestJson, error) {
	return agent.mgmt.GetCollectionManifest(ctx, opts)
}

func (agent *Agent) CreateScope(ctx context.Context, opts *cbmgmtx.CreateScopeOptions) error {
	return agent.mgmt.CreateScope(ctx, opts)
}

func (agent *Agent) DeleteScope(ctx context.Context, opts *cbmgmtx.DeleteScopeOptions) error {
	return agent.mgmt.DeleteScope(ctx, opts)
}

func (agent *Agent) CreateCollection(ctx context.Context, opts *cbmgmtx.CreateCollectionOptions) error {
	return agent.mgmt.CreateCollection(ctx, opts)
}

func (agent *Agent) DeleteCollection(ctx context.Context, opts *cbmgmtx.DeleteCollectionOptions) error {
	return agent.mgmt.DeleteCollection(ctx, opts)
}

func (agent *Agent) GetAllBuckets(ctx context.Context, opts *cbmgmtx.GetAllBucketsOptions) ([]*cbmgmtx.BucketDef, error) {
	return agent.mgmt.GetAllBuckets(ctx, opts)
}

func (agent *Agent) GetBucket(ctx context.Context, opts *cbmgmtx.GetBucketOptions) (*cbmgmtx.BucketDef, error) {
	return agent.mgmt.GetBucket(ctx, opts)
}

func (agent *Agent) CreateBucket(ctx context.Context, opts *cbmgmtx.CreateBucketOptions) error {
	return agent.mgmt.CreateBucket(ctx, opts)
}

func (agent *Agent) UpdateBucket(ctx context.Context, opts *cbmgmtx.UpdateBucketOptions) error {
	return agent.mgmt.UpdateBucket(ctx, opts)
}

func (agent *Agent) FlushBucket(ctx context.Context, opts *cbmgmtx.FlushBucketOptions) error {
	return agent.mgmt.FlushBucket(ctx, opts)
}

func (agent *Agent) DeleteBucket(ctx context.Context, opts *cbmgmtx.DeleteBucketOptions) error {
	return agent.mgmt.DeleteBucket(ctx, opts)
}
