package atomiccowcache

import (
	"sync"
	"sync/atomic"
)

type cacheData[K comparable, V any] struct {
	data map[K]V
}

type Cache[K comparable, V any] struct {
	gen func(K) V

	fastMap  atomic.Pointer[cacheData[K, V]]
	slowLock sync.Mutex
	slowMap  map[K]V
}

func NewCache[K comparable, V any](gen func(K) V) *Cache[K, V] {
	c := &Cache[K, V]{
		gen:     gen,
		slowMap: make(map[K]V),
	}
	c.rebuildFastMapLocked()
	return c
}

func (c *Cache[K, V]) rebuildFastMapLocked() {
	newFastMap := make(map[K]V)
	for k, v := range c.slowMap {
		newFastMap[k] = v
	}
	c.fastMap.Store(&cacheData[K, V]{
		data: newFastMap,
	})
}

func (c *Cache[K, V]) Get(k K) V {
	fastMap := c.fastMap.Load().data
	if v, ok := fastMap[k]; ok {
		return v
	}

	c.slowLock.Lock()

	if v, ok := c.slowMap[k]; ok {
		c.slowLock.Unlock()
		return v
	}

	v := c.gen(k)
	c.slowMap[k] = v
	c.rebuildFastMapLocked()

	c.slowLock.Unlock()

	return v
}
