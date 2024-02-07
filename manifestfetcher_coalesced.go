package gocbcorex

import (
	"context"
	"errors"
	"sync"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
)

type manifestFetch struct {
	CheckRes   *cbconfig.CollectionManifestJson
	CheckErr   error
	NeedsFetch bool
	PendingCh  chan struct{}
}

type ManifestFetcherCoalesced struct {
	Fetcher ManifestFetcher

	lock    sync.Mutex
	buckets map[string]*manifestFetch
}

var _ ManifestFetcher = (*ManifestFetcherCoalesced)(nil)

type ManifestFetcherCoalescedOptions struct {
	Fetcher ManifestFetcher
}

func NewManifestFetcherCoalesced(opts *ManifestFetcherCoalescedOptions) *ManifestFetcherCoalesced {
	return &ManifestFetcherCoalesced{
		Fetcher: opts.Fetcher,
		buckets: make(map[string]*manifestFetch),
	}
}

func (m *ManifestFetcherCoalesced) manifestFetchThread(bucketName string, fetch *manifestFetch) {
	m.lock.Lock()
	nextFetch := &manifestFetch{
		NeedsFetch: false,
		PendingCh:  make(chan struct{}),
	}
	m.buckets[bucketName] = nextFetch
	m.lock.Unlock()

	resp, err := m.Fetcher.GetManifest(context.Background(), bucketName)

	m.lock.Lock()

	fetch.CheckRes = resp
	fetch.CheckErr = err

	pendingCh := fetch.PendingCh
	fetch.PendingCh = nil

	nextNeedsFetch := nextFetch.NeedsFetch

	if !nextNeedsFetch {
		delete(m.buckets, bucketName)
	}

	m.lock.Unlock()

	close(pendingCh)

	if nextNeedsFetch {
		m.manifestFetchThread(bucketName, nextFetch)
	}
}

func (m *ManifestFetcherCoalesced) GetManifest(ctx context.Context, bucketName string) (*cbconfig.CollectionManifestJson, error) {
	m.lock.Lock()

	var pendingCh chan struct{}

	fetch := m.buckets[bucketName]
	if fetch == nil {
		fetch = &manifestFetch{
			PendingCh:  make(chan struct{}),
			NeedsFetch: true,
		}
		m.buckets[bucketName] = fetch

		pendingCh = fetch.PendingCh

		m.lock.Unlock()

		go m.manifestFetchThread(bucketName, fetch)
	} else {
		fetch.NeedsFetch = true
		pendingCh = fetch.PendingCh

		m.lock.Unlock()
	}

	if pendingCh == nil {
		return nil, errors.New("manifest fetcher pending channel was invalid")
	}

	select {
	case <-pendingCh:
		return fetch.CheckRes, fetch.CheckErr
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
