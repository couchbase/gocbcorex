package cbauthx

import (
	"context"
	"crypto/sha512"
	"crypto/x509"
	"sync"
	"sync/atomic"
	"time"
)

type HashedCert [32]byte

type certCacheEntry struct {
	ResolveErr error
	Info       *UserInfo
	When       time.Time

	// PendingCh represents a channel that can be listened to to know
	// when the auth check  has been completed (with either a success or a
	// failure).
	PendingCh chan struct{}
}

type certCacheFastCache struct {
	usersByCert map[HashedCert]*UserInfo
}

type CertCheckCached struct {
	certChecker CertCheck

	fastCache atomic.Pointer[certCacheFastCache]

	slowLock  sync.Mutex
	slowCache map[HashedCert]*certCacheEntry
}

var _ CertCheck = (*CertCheckCached)(nil)

func NewCertCheckCached(certChecker CertCheck) *CertCheckCached {
	return &CertCheckCached{
		certChecker: certChecker,
	}
}

func (cr *CertCheckCached) rebuildFastCacheLocked() {
	fastCache := &certCacheFastCache{
		usersByCert: make(map[HashedCert]*UserInfo),
	}

	if cr.slowCache != nil {
		for key, entry := range cr.slowCache {
			if entry.Info != nil {
				fastCache.usersByCert[key] = entry.Info
			}
		}
	}

	cr.fastCache.Store(fastCache)
}

func (a *CertCheckCached) CheckCertificate(ctx context.Context, clientCert *x509.Certificate) (UserInfo, error) {
	certHash := sha512.Sum512_256(clientCert.Raw)

	fastCertCache := a.fastCache.Load()
	if fastCertCache != nil {
		userInfo, wasFound := fastCertCache.usersByCert[certHash]
		if wasFound {
			return *userInfo, nil
		}
	}

	return a.checkSlow(ctx, clientCert, certHash)
}

func (a *CertCheckCached) checkSlow(
	ctx context.Context,
	clientCert *x509.Certificate,
	hashedCert HashedCert) (UserInfo, error) {
	a.slowLock.Lock()

	if a.slowCache == nil {
		a.slowCache = make(map[HashedCert]*certCacheEntry)
	}

	slowEntry, hasSlowEntry := a.slowCache[hashedCert]
	if !hasSlowEntry {
		slowEntry = &certCacheEntry{
			When:      time.Now(),
			PendingCh: make(chan struct{}),
		}
		a.slowCache[hashedCert] = slowEntry

		go a.checkThread(hashedCert, slowEntry, clientCert)
	}

	pendingCh := slowEntry.PendingCh

	a.slowLock.Unlock()

	if pendingCh != nil {
		select {
		case <-pendingCh:
		case <-ctx.Done():
			return UserInfo{}, &contextualError{
				Message: "context cancelled during background client cert check",
				Cause:   ctx.Err(),
			}
		}
	}

	// pendingCh is guaranteed to only be closed after these fields have been
	// written so it is safe to read them having blocked on pendingCh above.
	resolveErr := slowEntry.ResolveErr
	resolveInfo := slowEntry.Info

	if resolveErr != nil {
		return UserInfo{}, resolveErr
	}

	return *resolveInfo, nil
}

func (a *CertCheckCached) checkThread(entryKey HashedCert, slowEntry *certCacheEntry, clientCert *x509.Certificate) {
	info, err := a.certChecker.CheckCertificate(context.Background(), clientCert)

	a.slowLock.Lock()

	if err != nil {
		// if an error occured, we record the error for downstream consumers
		// and then remove it from the map so that future requests kick off
		// a new validation against the server.
		slowEntry.ResolveErr = err

		// We check that the entry still matches before deleting it, in case
		// an invalidation occurred since the entry was added to the map.
		if a.slowCache[entryKey] == slowEntry {
			delete(a.slowCache, entryKey)
		}
	} else {
		// If this particular check is being executed for a user which is not
		// in the local or admin domain (it's an external user), we need to
		// immediately remove them from the cache, since these entries are not
		// safe to cache and must be looked up each time.
		if info.Domain != "local" && info.Domain != "admin" {
			if a.slowCache[entryKey] == slowEntry {
				delete(a.slowCache, entryKey)
			}
		}

		// if we got a successful response, we can just record the data and
		// keep the entry until it is invalidated.
		slowEntry.Info = &info
	}

	pendingCh := slowEntry.PendingCh

	// Set the pending channel for the slow entry to nil to prevent checkThreads
	// against the same slow entry from trying to close a closed channel.
	slowEntry.PendingCh = nil

	a.rebuildFastCacheLocked()
	a.slowLock.Unlock()

	if pendingCh != nil {
		close(pendingCh)
	}
}

func (a *CertCheckCached) Invalidate() {
	a.slowLock.Lock()
	a.slowCache = make(map[HashedCert]*certCacheEntry)
	a.rebuildFastCacheLocked()
	a.slowLock.Unlock()
}
