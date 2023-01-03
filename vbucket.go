package core

type VbucketDispatcher interface {
	DispatchByKey(ctx *AsyncContext, key []byte) (string, error)
	DispatchToVbucket(ctx *AsyncContext, vbID uint16) (string, error)
}

// type vbucketDispatcher struct {
// 	vbmap unsafe.Pointer
// }
//
// func (vbd *vbucketDispatcher) DispatchByKey(ctx *asyncContext, key []byte) (string, error) {
// 	vbmap := vbd.LoadVbMap()
// 	if vbmap == nil {
// 		return "", placeholderError{"imnotgood"}
// 	}
//
// 	return vbmap.NodeByKey(key, 0)
// }
//
// func (vbd *vbucketDispatcher) DispatchToVbucket(ctx *asyncContext, vbID uint16) (string, error) {
// 	vbmap := vbd.LoadVbMap()
// 	if vbmap == nil {
// 		return "", placeholderError{"imnotgood"}
// 	}
//
// 	return vbmap.NodeByVbucket(vbID, 0)
// }
//
// func (vbd *vbucketDispatcher) LoadVbMap() *vbucketMap {
// 	vbmap := (*vbucketMap)(atomic.LoadPointer(&vbd.vbmap))
// 	return vbmap
// }
//
// func (vbd *vbucketDispatcher) StoreVbMap(old, new *vbucketMap) bool {
// 	return atomic.CompareAndSwapPointer(&vbd.vbmap, unsafe.Pointer(old), unsafe.Pointer(new))
// }
