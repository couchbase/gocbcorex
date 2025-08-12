package cbauthx

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type certCacheFastEntry struct {
	HashedPassword [32]byte
	Info           *UserInfo
}

type certCacheKey struct {
	Username       string
	HashedPassword [32]byte
}

type certCacheEntry struct {
	ResolveErr error
	Info       *UserInfo
	When       time.Time

	// PendingCh represents a channel that can be listened to to know
	// when the collection resolution has been completed (with either
	// a success or a failure).
	PendingCh chan struct{}
}

type certCacheFastCache struct {
	users map[string]certCacheFastEntry
}

type CertCheckCached struct {
	certChecker CertCheck

	fastCache atomic.Pointer[certCacheFastCache]

	slowLock  sync.Mutex
	slowCache map[certCacheKey]*certCacheEntry
}

var _ CertCheck = (*CertCheckCached)(nil)

func NewCertCheckCached(certChecker CertCheck) *CertCheckCached {
	return &CertCheckCached{
		certChecker: certChecker,
	}
}

func (cr *CertCheckCached) rebuildFastCacheLocked() {
	fastCache := &certCacheFastCache{
		users: make(map[string]certCacheFastEntry),
	}

	if cr.slowCache != nil {
		for key, entry := range cr.slowCache {
			if entry.Info != nil {
				fastCache.users[key.Username] = certCacheFastEntry{
					HashedPassword: key.HashedPassword,
					Info:           entry.Info,
				}
			}
		}
	}

	cr.fastCache.Store(fastCache)
}

func (a *CertCheckCached) CheckCertificate(ctx context.Context, connState *tls.ConnectionState) (UserInfo, error) {
	// TODO(brett19): Implement cacheing

	return a.checkSlow(ctx, connState)
}

func (a *CertCheckCached) checkSlow(ctx context.Context, connState *tls.ConnectionState) (UserInfo, error) {
	// TODO(brett19): Implement slow checking
	return UserInfo{}, errors.New("not implemented")
}

func (a *CertCheckCached) Invalidate() {
	a.slowLock.Lock()
	a.slowCache = make(map[certCacheKey]*certCacheEntry)
	a.rebuildFastCacheLocked()
	a.slowLock.Unlock()
}
