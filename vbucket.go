package core

type VbucketDispatcher interface {
	DispatchByKey(ctx *AsyncContext, key []byte, cb func(string, error))
	DispatchToVbucket(ctx *AsyncContext, vbID uint16, cb func(string, error))
}
