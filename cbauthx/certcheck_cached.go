package cbauthx

import (
	"context"
	"crypto/sha512"
	"crypto/tls"
	"errors"
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
	// when the collection resolution has been completed (with either
	// a success or a failure).
	PendingCh chan struct{}
}

type certCacheFastCache struct {
	users map[HashedCert]*UserInfo
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
		users: make(map[HashedCert]*UserInfo),
	}

	if cr.slowCache != nil {
		for key, entry := range cr.slowCache {
			if entry.Info != nil {
				fastCache.users[key] = entry.Info
			}
		}
	}

	cr.fastCache.Store(fastCache)
}

func (a *CertCheckCached) CheckCertificate(ctx context.Context, connState *tls.ConnectionState) (UserInfo, error) {
	certHash, err := certHashFromConnState(connState)
	if err != nil {
		return UserInfo{}, &contextualError{
			Message: "cannot generate cert hash from tls connection",
			Cause:   err,
		}
	}

	fastCertCache := a.fastCache.Load()
	if fastCertCache != nil {
		userInfo, wasFound := fastCertCache.users[certHash]
		if wasFound {
			return *userInfo, nil
		}
	}

	return a.checkSlow(ctx, connState)
}

func (a *CertCheckCached) checkSlow(ctx context.Context, connState *tls.ConnectionState) (UserInfo, error) {
	a.slowLock.Lock()

	if a.slowCache == nil {
		a.slowCache = make(map[HashedCert]*certCacheEntry)
	}

	hashedCert, err := certHashFromConnState(connState)
	if err != nil {
		a.slowLock.Unlock()
		return UserInfo{}, &contextualError{
			Message: "cannot generate cert hash from tls connection",
			Cause:   err,
		}
	}

	slowEntry, hasSlowEntry := a.slowCache[hashedCert]
	if !hasSlowEntry {
		slowEntry = &certCacheEntry{
			When:      time.Now(),
			PendingCh: make(chan struct{}),
		}
		a.slowCache[hashedCert] = slowEntry

		go a.checkThread(hashedCert, slowEntry, connState)
	}

	if slowEntry.PendingCh != nil {
		pendingCh := slowEntry.PendingCh

		a.slowLock.Unlock()

		select {
		case <-pendingCh:
		case <-ctx.Done():
			return UserInfo{}, &contextualError{
				Message: "context cancelled during background client cert check",
				Cause:   ctx.Err(),
			}
		}
	}

	if slowEntry.ResolveErr != nil {
		return UserInfo{}, slowEntry.ResolveErr
	}

	return *slowEntry.Info, nil
}

func (a *CertCheckCached) checkThread(entryKey HashedCert, slowEntry *certCacheEntry, connState *tls.ConnectionState) {
	info, err := a.certChecker.CheckCertificate(context.Background(), connState)

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

func certHashFromConnState(connState *tls.ConnectionState) (HashedCert, error) {
	if len(connState.PeerCertificates) == 0 {
		return HashedCert{}, errors.New("no client certificate provided")
	}

	return sha512.Sum512_256(connState.PeerCertificates[0].Raw), nil
}
