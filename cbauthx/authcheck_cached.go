package cbauthx

import (
	"context"
	"crypto/sha512"
	"sync"
	"sync/atomic"
	"time"
)

type authCacheFastEntry struct {
	HashedPassword [32]byte
	Info           *UserInfo
}

type authCacheKey struct {
	Username       string
	HashedPassword [32]byte
}

type authCacheEntry struct {
	ResolveErr error
	Info       *UserInfo
	When       time.Time

	// PendingCh represents a channel that can be listened to to know
	// when the collection resolution has been completed (with either
	// a success or a failure).
	PendingCh chan struct{}
}

type authCacheFastCache struct {
	users map[string]authCacheFastEntry
}

type AuthCheckCached struct {
	authChecker AuthCheck

	fastCache atomic.Pointer[authCacheFastCache]

	slowLock  sync.Mutex
	slowCache map[authCacheKey]*authCacheEntry
}

var _ AuthCheck = (*AuthCheckCached)(nil)

func NewAuthCheckCached(authChecker AuthCheck) *AuthCheckCached {
	return &AuthCheckCached{
		authChecker: authChecker,
	}
}

func (cr *AuthCheckCached) rebuildFastCacheLocked() {
	fastCache := &authCacheFastCache{
		users: make(map[string]authCacheFastEntry),
	}

	if cr.slowCache != nil {
		for key, entry := range cr.slowCache {
			if entry.Info != nil {
				fastCache.users[key.Username] = authCacheFastEntry{
					HashedPassword: key.HashedPassword,
					Info:           entry.Info,
				}
			}
		}
	}

	cr.fastCache.Store(fastCache)
}

func (a *AuthCheckCached) CheckUserPass(ctx context.Context, username string, password string) (UserInfo, error) {
	fastAuthCache := a.fastCache.Load()
	if fastAuthCache != nil {
		authEntry, wasFound := fastAuthCache.users[username]
		if wasFound {
			hashedPassword := sha512.Sum512_256([]byte(password))
			if authEntry.HashedPassword != hashedPassword {
				return UserInfo{}, ErrInvalidAuth
			}

			return *authEntry.Info, nil
		}
	}

	return a.checkSlow(ctx, username, password)
}

func (a *AuthCheckCached) checkSlow(ctx context.Context, username string, password string) (UserInfo, error) {
	a.slowLock.Lock()

	if a.slowCache == nil {
		a.slowCache = make(map[authCacheKey]*authCacheEntry)
	}

	hashedPassword := sha512.Sum512_256([]byte(password))
	key := authCacheKey{
		Username:       username,
		HashedPassword: hashedPassword,
	}

	slowEntry, hasSlowEntry := a.slowCache[key]
	if !hasSlowEntry {
		slowEntry = &authCacheEntry{
			When:      time.Now(),
			PendingCh: make(chan struct{}),
		}
		a.slowCache[key] = slowEntry

		go a.checkThread(
			key, slowEntry, username, password)
	}

	if slowEntry.PendingCh != nil {
		pendingCh := slowEntry.PendingCh

		a.slowLock.Unlock()

		select {
		case <-pendingCh:
		case <-ctx.Done():
			return UserInfo{}, &contextualError{
				Message: "context cancelled during background auth check",
				Cause:   ctx.Err(),
			}
		}
	}

	if slowEntry.ResolveErr != nil {
		return UserInfo{}, slowEntry.ResolveErr
	}

	return *slowEntry.Info, nil
}

func (a *AuthCheckCached) checkThread(
	entryKey authCacheKey,
	slowEntry *authCacheEntry,
	username string, password string,
) {
	info, err := a.authChecker.CheckUserPass(context.Background(), username, password)

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

func (a *AuthCheckCached) Invalidate() {
	a.slowLock.Lock()
	a.slowCache = make(map[authCacheKey]*authCacheEntry)
	a.rebuildFastCacheLocked()
	a.slowLock.Unlock()
}
