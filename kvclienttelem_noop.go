package gocbcorex

import (
	"context"
	"time"
)

type kvClientTelemNoOp struct {
}

var _ MemdClientTelem = (*kvClientTelemNoOp)(nil)

func newKvClientTelemNoOp() *kvClientTelemNoOp {
	return &kvClientTelemNoOp{}
}

type kvClientTelemOpNoOp struct {
}

func (k *kvClientTelemNoOp) BeginOp(
	ctx context.Context,
	bucketName string,
	opName string,
) (context.Context, MemdClientTelemOp) {

	return ctx, &kvClientTelemOpNoOp{}
}

func (k *kvClientTelemOpNoOp) IsRecording() bool {
	return false
}

func (k *kvClientTelemOpNoOp) MarkSent() {
}

func (k *kvClientTelemOpNoOp) MarkReceived() {
}

func (k *kvClientTelemOpNoOp) RecordServerDuration(d time.Duration) {
}

func (k *kvClientTelemOpNoOp) End(ctx context.Context, err error) {
}
