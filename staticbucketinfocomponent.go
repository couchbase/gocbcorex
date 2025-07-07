package gocbcorex

import (
	"context"
	"sync"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"go.uber.org/zap"
)

type staticBucketInfoState struct {
	conflictResolutionMode cbmgmtx.ConflictResolutionType
}

type StaticBucketInfoComponent struct {
	logger *zap.Logger
	mgmt   *MgmtComponent

	lock       sync.Mutex
	bucketName string
	state      *staticBucketInfoState
}

type StaticBucketInfoComponentConfig struct {
	BucketName string
}

type StaticBucketInfoComponentOptions struct {
	Logger *zap.Logger
}

func NewStaticBucketInfoComponent(
	mgmt *MgmtComponent,
	config *StaticBucketInfoComponentConfig,
	opts *StaticBucketInfoComponentOptions,
) *StaticBucketInfoComponent {
	return &StaticBucketInfoComponent{
		logger:     opts.Logger,
		mgmt:       mgmt,
		bucketName: config.BucketName,
	}
}

func (c *StaticBucketInfoComponent) Reconfigure(config *StaticBucketInfoComponentConfig) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.bucketName != config.BucketName {
		c.bucketName = config.BucketName
		c.state = nil // reset state to force a reload
	}

	return nil
}

func (c *StaticBucketInfoComponent) getState(ctx context.Context) (*staticBucketInfoState, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for c.state == nil {
		bucketName := c.bucketName
		if bucketName == "" {
			return nil, ErrNoBucketSelected
		}

		c.lock.Unlock()
		bucketDef, err := c.mgmt.GetBucket(ctx, &cbmgmtx.GetBucketOptions{
			BucketName: bucketName,
		})
		c.lock.Lock()
		if err != nil {
			return nil, err
		}

		// double check the bucket name has not changed
		if c.bucketName != bucketName {
			continue
		}

		c.state = &staticBucketInfoState{
			conflictResolutionMode: bucketDef.ConflictResolutionType,
		}
	}

	return c.state, nil
}

func (c *StaticBucketInfoComponent) GetConflictResolutionMode(ctx context.Context) (cbmgmtx.ConflictResolutionType, error) {
	state, err := c.getState(ctx)
	if err != nil {
		return cbmgmtx.ConflictResolutionTypeUnset, err
	}

	return state.conflictResolutionMode, nil
}
