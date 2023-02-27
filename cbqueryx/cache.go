package cbqueryx

import "sync"

type PreparedStatementCache struct {
	queryCache map[string]string
	cacheLock  sync.RWMutex
}

func NewPreparedStatementCache() *PreparedStatementCache {
	return &PreparedStatementCache{
		queryCache: make(map[string]string),
	}
}

func (cache *PreparedStatementCache) Get(statement string) (string, bool) {
	cache.cacheLock.RLock()
	entry, ok := cache.queryCache[statement]
	cache.cacheLock.RUnlock()

	return entry, ok
}

func (cache *PreparedStatementCache) Put(statement, preparedName string) {
	cache.cacheLock.Lock()
	cache.queryCache[statement] = preparedName
	cache.cacheLock.Unlock()
}
