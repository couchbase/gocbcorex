package gocbcorex_test

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"

	"github.com/stretchr/testify/assert"

	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/require"

	"go.uber.org/zap"
)

func TestBucketsWatcherHttp(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	auth := &gocbcorex.PasswordAuthenticator{
		Username: testutilsint.TestOpts.Username,
		Password: testutilsint.TestOpts.Password,
	}
	var seeds []string
	for _, addr := range testutilsint.TestOpts.HTTPAddrs {
		seeds = append(seeds, "http://"+addr)
	}

	newCfg := func(tripper *ForwardingHttpRoundTripper) gocbcorex.BucketsWatcherHttpConfig {
		return gocbcorex.BucketsWatcherHttpConfig{
			HttpRoundTripper: tripper,
			Endpoints:        seeds,
			UserAgent:        "gocbcorex-test",
			Authenticator:    auth,
			MakeAgent: func(ctx context.Context, bucketName string) (*gocbcorex.Agent, error) {
				opts := CreateDefaultAgentOptions()
				opts.BucketName = bucketName

				return gocbcorex.CreateAgent(ctx, opts)
			},
		}
	}

	t.Run("HandlesStreamedBuckets", func(tt *testing.T) {
		tripper := NewForwardingHttpRoundTripper(nil)
		defer tripper.Close()

		watcher, err := gocbcorex.NewBucketsWatcherHttp(newCfg(tripper), gocbcorex.BucketsWatcherHttpOptions{Logger: logger})
		require.NoError(tt, err)

		watcher.Watch()

		if assert.Eventuallyf(tt, func() bool {
			return watcher.NumAgents() > 0
		}, 5*time.Second, 100*time.Millisecond, "Watcher failed to create any agents in time specified") {

			_, err = watcher.GetAgent(context.Background(), testutilsint.TestOpts.BucketName)
			require.NoError(tt, err)
		}

		err = watcher.Close()
		require.NoError(tt, err)

		// There should only have been 1 request, the stream open.
		assert.Equal(tt, 1, tripper.NumReqs())

	})

	t.Run("FetchesUnknownBuckets", func(tt *testing.T) {
		tripper := NewForwardingHttpRoundTripper(nil)
		defer tripper.Close()

		watcher, err := gocbcorex.NewBucketsWatcherHttp(newCfg(tripper), gocbcorex.BucketsWatcherHttpOptions{Logger: logger})
		require.NoError(tt, err)

		// Don't start the watcher, to force a http fetch.

		_, err = watcher.GetAgent(context.Background(), testutilsint.TestOpts.BucketName)
		require.NoError(tt, err)

		err = watcher.Close()
		require.NoError(tt, err)

		// There should only have been 1 request.
		assert.Equal(tt, 1, tripper.NumReqs())
	})

	t.Run("OnlyFetchesABucketOnce", func(tt *testing.T) {
		tripper := NewForwardingHttpRoundTripper(nil)
		defer tripper.Close()

		watcher, err := gocbcorex.NewBucketsWatcherHttp(newCfg(tripper), gocbcorex.BucketsWatcherHttpOptions{Logger: logger})
		require.NoError(tt, err)

		// Don't start the watcher, to force a http fetch.

		agent, err := watcher.GetAgent(context.Background(), testutilsint.TestOpts.BucketName)
		require.NoError(tt, err)

		agent2, err := watcher.GetAgent(context.Background(), testutilsint.TestOpts.BucketName)
		require.NoError(tt, err)

		assert.Equal(tt, agent, agent2)

		err = watcher.Close()
		require.NoError(tt, err)

		// There should only have been 1 request.
		assert.Equal(tt, 1, tripper.NumReqs())
	})

	t.Run("OnlyFetchesABucketOnceWithConcurrency", func(tt *testing.T) {
		tripper := NewForwardingHttpRoundTripper(nil)
		defer tripper.Close()

		watcher, err := gocbcorex.NewBucketsWatcherHttp(newCfg(tripper), gocbcorex.BucketsWatcherHttpOptions{Logger: logger})
		require.NoError(tt, err)

		// Don't start the watcher, to force a http fetch.

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			_, err := watcher.GetAgent(context.Background(), testutilsint.TestOpts.BucketName)
			require.NoError(tt, err)
			wg.Done()
		}()

		go func() {
			_, err := watcher.GetAgent(context.Background(), testutilsint.TestOpts.BucketName)
			require.NoError(tt, err)
			wg.Done()
		}()

		wg.Wait()

		err = watcher.Close()
		require.NoError(tt, err)

		// There should only have been 1 request.
		assert.Equal(tt, 1, tripper.NumReqs())
	})

	t.Run("UnknownBucketWithConcurrencyAfterFirstRequestIssued", func(tt *testing.T) {
		controlCh := make(chan struct{})
		numResps := 0
		tripperFn := func(resp *http.Response) {
			numResps++
			if numResps == 1 {
				controlCh <- struct{}{}
			}
		}
		tripper := NewForwardingHttpRoundTripper(tripperFn)
		defer tripper.Close()

		watcher, err := gocbcorex.NewBucketsWatcherHttp(newCfg(tripper), gocbcorex.BucketsWatcherHttpOptions{Logger: logger})
		require.NoError(tt, err)

		// Don't start the watcher, to force a http fetch.

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			_, err := watcher.GetAgent(context.Background(), testutilsint.TestOpts.BucketName)
			require.NoError(tt, err)
			wg.Done()
		}()

		go func() {
			<-controlCh
			_, err := watcher.GetAgent(context.Background(), "notafish")
			require.ErrorIs(tt, err, cbmgmtx.ErrBucketNotFound)
			wg.Done()
		}()

		wg.Wait()

		err = watcher.Close()
		require.NoError(tt, err)

		// There should have been 2 requests, as we issued the second GetAgent whilst the first was already in flight.
		assert.Equal(tt, 2, tripper.NumReqs())
	})
}
