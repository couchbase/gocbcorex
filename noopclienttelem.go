package gocbcorex

import (
	"context"
	"time"
)

type noopClientTelem struct {
}

var _ MemdClientTelem = (*noopClientTelem)(nil)

type noopClientTelemOp struct {
}

func (k *noopClientTelem) BeginOp(
	ctx context.Context,
	bucketName string,
	opName string,
) (context.Context, MemdClientTelemOp) {
	return ctx, &noopClientTelemOp{}
}

func (k *noopClientTelemOp) IsRecording() bool {
	return false
}

func (k *noopClientTelemOp) MarkSent() {
}

func (k *noopClientTelemOp) MarkReceived() {
}

func (k *noopClientTelemOp) RecordServerDuration(d time.Duration) {
}

func (k *noopClientTelemOp) End(ctx context.Context, err error) {
}
