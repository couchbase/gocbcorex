package gocbcorex

import (
	"context"
	"fmt"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

type KvClientDcpEventsHandlers struct {
	SnapshotMarker     func(req *memdx.DcpSnapshotMarkerEvent) error
	Mutation           func(req *memdx.DcpMutationEvent) error
	Deletion           func(req *memdx.DcpDeletionEvent) error
	Expiration         func(req *memdx.DcpExpirationEvent) error
	CollectionCreation func(req *memdx.DcpCollectionCreationEvent) error
	CollectionDeletion func(req *memdx.DcpCollectionDeletionEvent) error
	CollectionFlush    func(req *memdx.DcpCollectionFlushEvent) error
	ScopeCreation      func(req *memdx.DcpScopeCreationEvent) error
	ScopeDeletion      func(req *memdx.DcpScopeDeletionEvent) error
	CollectionChanged  func(req *memdx.DcpCollectionModificationEvent) error
	StreamEnd          func(req *memdx.DcpStreamEndEvent) error
	OSOSnapshot        func(req *memdx.DcpOSOSnapshotEvent) error
	SeqNoAdvanced      func(req *memdx.DcpSeqNoAdvancedEvent) error
}

type KvClientDcpOptions struct {
	ConnectionName        string
	ConsumerName          string
	ConnectionFlags       memdx.DcpConnectionFlags
	NoopInterval          time.Duration
	Priority              string
	ForceValueCompression bool
	EnableExpiryEvents    bool
	EnableStreamIds       bool
	EnableOso             bool
	EnableSeqNoAdvance    bool
	BackfillOrder         string
	EnableChangeStreams   bool
}

func (o KvClientDcpOptions) Equals(b KvClientDcpOptions) bool {
	return o.ConnectionName == b.ConnectionName &&
		o.ConsumerName == b.ConsumerName &&
		o.ConnectionFlags == b.ConnectionFlags &&
		o.NoopInterval == b.NoopInterval &&
		o.Priority == b.Priority &&
		o.ForceValueCompression == b.ForceValueCompression &&
		o.EnableExpiryEvents == b.EnableExpiryEvents &&
		o.EnableStreamIds == b.EnableStreamIds &&
		o.EnableOso == b.EnableOso &&
		o.EnableSeqNoAdvance == b.EnableSeqNoAdvance &&
		o.BackfillOrder == b.BackfillOrder &&
		o.EnableChangeStreams == b.EnableChangeStreams
}

type KvClientDcpState struct {
	noopEnabled                  bool
	streamEndOnCloseEnabled      bool
	priority                     string
	forceValueCompressionEnabled bool
	expiryEventsEnabled          bool
	streamIdsEnabled             bool
	osoEnabled                   bool
	seqNoAdvancedEnabled         bool
	backfillOrder                string
	changeStreamsEnabled         bool
}

func (c *kvClient) bootstrapDcp(
	ctx context.Context,
	dcpOpts *KvClientDcpOptions,
) (*KvClientDcpState, error) {
	c.dcpState = &KvClientDcpState{}

	_, err := c.dcpOpenConnection(ctx, &memdx.DcpOpenConnectionRequest{
		ConnectionName: dcpOpts.ConnectionName,
		ConsumerName:   dcpOpts.ConsumerName,
		Flags:          dcpOpts.ConnectionFlags,
	})
	if err != nil {
		return nil, contextualError{
			Message: "dcp openconnection failed",
			Cause:   err,
		}
	}

	_, err = c.dcpControl(ctx, &memdx.DcpControlRequest{
		Key:   "send_stream_end_on_client_close_stream",
		Value: "true",
	})
	if err != nil {
		c.logger.Debug("failed to enable stream-end-on-close feature", zap.Error(err))
	} else {
		c.dcpState.streamEndOnCloseEnabled = true
	}

	if dcpOpts.NoopInterval > 0 {
		_, err = c.dcpControl(ctx, &memdx.DcpControlRequest{
			Key:   "set_noop_interval",
			Value: fmt.Sprintf("%d", dcpOpts.NoopInterval/time.Second),
		})
		if err == nil {
			_, err = c.dcpControl(ctx, &memdx.DcpControlRequest{
				Key:   "enable_noop",
				Value: "true",
			})
		}
		if err != nil {
			c.logger.Debug("noop requested, but could not be enabled", zap.Error(err))
		} else {
			c.dcpState.noopEnabled = true
		}
	}

	if dcpOpts.Priority != "" {
		_, err = c.dcpControl(ctx, &memdx.DcpControlRequest{
			Key:   "set_priority",
			Value: dcpOpts.Priority,
		})
		if err != nil {
			c.logger.Debug("failed to set dcp priority", zap.Error(err))
		} else {
			c.dcpState.priority = dcpOpts.Priority
		}
	}

	if dcpOpts.ForceValueCompression {
		_, err = c.dcpControl(ctx, &memdx.DcpControlRequest{
			Key:   "force_value_compression",
			Value: "true",
		})
		if err != nil {
			c.logger.Debug("failed to enable forced value compression", zap.Error(err))
		} else {
			c.dcpState.forceValueCompressionEnabled = true
		}
	}

	if dcpOpts.EnableExpiryEvents {
		_, err = c.dcpControl(ctx, &memdx.DcpControlRequest{
			Key:   "enable_expiry_opcode",
			Value: "true",
		})
		if err != nil {
			c.logger.Debug("failed to enable expiry events feature", zap.Error(err))
		} else {
			c.dcpState.expiryEventsEnabled = true
		}
	}

	if dcpOpts.EnableStreamIds {
		_, err = c.dcpControl(ctx, &memdx.DcpControlRequest{
			Key:   "enable_stream_id",
			Value: "true",
		})
		if err != nil {
			c.logger.Debug("failed to enable stream ids", zap.Error(err))
		} else {
			c.dcpState.streamIdsEnabled = true
		}
	}

	if dcpOpts.EnableOso {
		if dcpOpts.EnableSeqNoAdvance {
			_, err = c.dcpControl(ctx, &memdx.DcpControlRequest{
				Key:   "enable_out_of_order_snapshots",
				Value: "true_with_seqno_advanced",
			})
			if err != nil {
				c.logger.Debug("failed to enable oso with seqno advanced", zap.Error(err))
			} else {
				c.dcpState.osoEnabled = true
				c.dcpState.seqNoAdvancedEnabled = true
			}
		}

		if !c.dcpState.osoEnabled {
			_, err = c.dcpControl(ctx, &memdx.DcpControlRequest{
				Key:   "enable_out_of_order_snapshots",
				Value: "true",
			})
			if err != nil {
				c.logger.Debug("failed to enable oso", zap.Error(err))
			} else {
				c.dcpState.osoEnabled = true
			}
		}
	}

	if dcpOpts.BackfillOrder != "" {
		_, err = c.dcpControl(ctx, &memdx.DcpControlRequest{
			Key:   "backfill_order",
			Value: dcpOpts.BackfillOrder,
		})
		if err != nil {
			c.logger.Debug("failed to set backfill order", zap.Error(err))
		} else {
			c.dcpState.backfillOrder = dcpOpts.BackfillOrder
		}
	}

	if dcpOpts.EnableChangeStreams {
		_, err = c.dcpControl(ctx, &memdx.DcpControlRequest{
			Key:   "change_streams",
			Value: "true",
		})
		if err != nil {
			c.logger.Debug("failed to enable change streams", zap.Error(err))
		} else {
			c.dcpState.changeStreamsEnabled = true
		}
	}

	return c.dcpState, nil
}
