package core

type VbucketDispatcher interface {
	DispatchByKey(ctx *asyncContext, key []byte, cb func(string, error))
	DispatchToVbucket(ctx *asyncContext, vbID uint16, cb func(string, error))
}
