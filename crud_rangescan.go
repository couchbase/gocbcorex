package gocbcorex

import (
	"context"
	"encoding/json"

	"github.com/couchbase/gocbcorex/memdx"
)

type RangeScanCreateOptions struct {
	ScopeName      string
	CollectionName string
	VbucketID      uint16

	Scan     json.RawMessage
	KeysOnly bool
	Range    *memdx.RangeScanCreateRangeScanConfig
	Sampling *memdx.RangeScanCreateRandomSamplingConfig
	Snapshot *memdx.RangeScanCreateSnapshotRequirements

	OnBehalfOf string
}

type RangeScanCreateResult struct {
	ScanUUUID []byte
}

func (cc *CrudComponent) RangeScanCreate(ctx context.Context, opts *RangeScanCreateOptions) (*RangeScanCreateResult, error) {
	return OrchestrateMemdRetries(
		ctx, cc.retries,
		func() (*RangeScanCreateResult, error) {
			return OrchestrateMemdCollectionID(
				ctx, cc.collections, opts.ScopeName, opts.CollectionName,
				func(collectionID uint32, manifestID uint64) (*RangeScanCreateResult, error) {
					endpoint, err := cc.vbs.DispatchToVbucket(opts.VbucketID)
					if err != nil {
						return nil, err
					}
					return OrchestrateMemdClient(ctx, cc.connManager, endpoint, func(client KvClient) (*RangeScanCreateResult, error) {
						res, err := client.RangeScanCreate(ctx, &memdx.RangeScanCreateRequest{
							CollectionID: collectionID,
							VbucketID:    opts.VbucketID,
							Scan:         opts.Scan,
							KeysOnly:     opts.KeysOnly,
							Range:        opts.Range,
							Sampling:     opts.Sampling,
							Snapshot:     opts.Snapshot,
							CrudRequestMeta: memdx.CrudRequestMeta{
								OnBehalfOf: opts.OnBehalfOf,
							},
						})
						if err != nil {
							return nil, err
						}

						return &RangeScanCreateResult{
							ScanUUUID: res.ScanUUUID,
						}, nil
					})
				})
		})
}

type RangeScanContinueOptions struct {
	ScanUUID  []byte
	MaxCount  uint32
	MaxBytes  uint32
	VbucketID uint16

	OnBehalfOf string
}

type RangeScanContinueDataResult struct {
	Items    []memdx.RangeScanItem
	KeysOnly bool
}

type RangeScanContinueResult struct {
	More     bool
	Complete bool
}

func (cc *CrudComponent) RangeScanContinue(ctx context.Context, opts *RangeScanContinueOptions,
	dataCb func(RangeScanContinueDataResult)) (*RangeScanContinueResult, error) {
	return OrchestrateMemdRetries(
		ctx, cc.retries,
		func() (*RangeScanContinueResult, error) {
			endpoint, err := cc.vbs.DispatchToVbucket(opts.VbucketID)
			if err != nil {
				return nil, err
			}
			return OrchestrateMemdClient(ctx, cc.connManager, endpoint, func(client KvClient) (*RangeScanContinueResult, error) {
				deadline, _ := ctx.Deadline()
				res, err := client.RangeScanContinue(ctx, &memdx.RangeScanContinueRequest{
					ScanUUID:  opts.ScanUUID,
					MaxCount:  opts.MaxCount,
					MaxBytes:  opts.MaxBytes,
					VbucketID: opts.VbucketID,
					Deadline:  deadline,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: opts.OnBehalfOf,
					},
				}, func(response *memdx.RangeScanDataResponse) error {
					// We don't use a for i, item range here so that we can modify the entry in place.
					for i := range response.Items {
						item := &response.Items[i]
						value, datatype, err := cc.compression.Decompress(memdx.DatatypeFlag(item.Datatype), item.Value)
						if err != nil {
							return err
						}

						(item).Value = value
						(item).Datatype = uint8(datatype)
					}
					dataCb(RangeScanContinueDataResult{
						Items:    response.Items,
						KeysOnly: response.KeysOnly,
					})

					return nil
				})
				if err != nil {
					return nil, err
				}

				return &RangeScanContinueResult{
					More:     res.More,
					Complete: res.Complete,
				}, nil
			})
		})
}

type RangeScanCancelOptions struct {
	ScanUUID  []byte
	VbucketID uint16

	OnBehalfOf string
}

type RangeScanCancelResult struct {
}

func (cc *CrudComponent) RangeScanCancel(ctx context.Context, opts *RangeScanCancelOptions) (*RangeScanCancelResult, error) {
	return OrchestrateMemdRetries(
		ctx, cc.retries,
		func() (*RangeScanCancelResult, error) {
			endpoint, err := cc.vbs.DispatchToVbucket(opts.VbucketID)
			if err != nil {
				return nil, err
			}
			return OrchestrateMemdClient(ctx, cc.connManager, endpoint, func(client KvClient) (*RangeScanCancelResult, error) {
				_, err := client.RangeScanCancel(ctx, &memdx.RangeScanCancelRequest{
					ScanUUID:  opts.ScanUUID,
					VbucketID: opts.VbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: opts.OnBehalfOf,
					},
				})
				if err != nil {
					return nil, err
				}

				return &RangeScanCancelResult{}, nil
			})
		})
}
