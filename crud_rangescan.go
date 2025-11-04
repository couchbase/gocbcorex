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
	ScanUUID []byte

	vbucketID uint16
	client    KvClient
	parent    *CrudComponent
}

func (cc *CrudComponent) RangeScanCreate(ctx context.Context, opts *RangeScanCreateOptions) (*RangeScanCreateResult, error) {
	return OrchestrateRetries(
		ctx, cc.retries,
		func() (*RangeScanCreateResult, error) {
			return OrchestrateMemdCollectionID(
				ctx, cc.collections, opts.ScopeName, opts.CollectionName,
				func(collectionID uint32, manifestID uint64) (*RangeScanCreateResult, error) {
					endpoint, err := cc.vbs.DispatchToVbucket(opts.VbucketID, 0)
					if err != nil {
						return nil, err
					}
					return OrchestrateEndpointKvClient(ctx, cc.clientProvider, endpoint, func(client KvClient) (*RangeScanCreateResult, error) {
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
							ScanUUID:  res.ScanUUUID,
							client:    client,
							vbucketID: opts.VbucketID,
							parent:    cc,
						}, nil
					})
				})
		})
}

type RangeScanContinueOptions struct {
	MaxCount uint32
	MaxBytes uint32

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

func (cr *RangeScanCreateResult) Continue(ctx context.Context, opts *RangeScanContinueOptions,
	dataCb func(RangeScanContinueDataResult)) (*RangeScanContinueResult, error) {
	return OrchestrateRetries(
		ctx, cr.parent.retries,
		func() (*RangeScanContinueResult, error) {
			deadline, _ := ctx.Deadline()
			res, err := cr.client.RangeScanContinue(ctx, &memdx.RangeScanContinueRequest{
				ScanUUID:  cr.ScanUUID,
				MaxCount:  opts.MaxCount,
				MaxBytes:  opts.MaxBytes,
				VbucketID: cr.vbucketID,
				Deadline:  deadline,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
			}, func(response *memdx.RangeScanDataResponse) error {
				// We don't use a for i, item range here so that we can modify the entry in place.
				for i := range response.Items {
					item := &response.Items[i]
					value, datatype, err := cr.parent.compression.Decompress(memdx.DatatypeFlag(item.Datatype), item.Value)
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
}

type RangeScanCancelOptions struct {
	OnBehalfOf string
}

type RangeScanCancelResult struct {
}

func (cr *RangeScanCreateResult) Cancel(ctx context.Context, opts *RangeScanCancelOptions) (*RangeScanCancelResult, error) {
	return OrchestrateRetries(
		ctx, cr.parent.retries,
		func() (*RangeScanCancelResult, error) {
			_, err := cr.client.RangeScanCancel(ctx, &memdx.RangeScanCancelRequest{
				ScanUUID:  cr.ScanUUID,
				VbucketID: cr.vbucketID,
				CrudRequestMeta: memdx.CrudRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
			})
			if err != nil {
				return nil, err
			}

			return &RangeScanCancelResult{}, nil
		})
}
