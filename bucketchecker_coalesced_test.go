package gocbcorex

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type bucketCheckResult struct {
	Result bool
	Error  error
}

func TestBucketChecker(t *testing.T) {
	resultCh := make(chan *bucketCheckResult, 128)
	checkSigCh := make(chan struct{}, 128)

	baseChecker := &BucketCheckerMock{
		HasBucketFunc: func(ctx context.Context, bucketName string) (bool, error) {
			log.Printf("HasBucketFunc")
			checkSigCh <- struct{}{}
			result := <-resultCh
			log.Printf("HasBucketFunc returning %t %s", result.Result, result.Error)
			return result.Result, result.Error
		},
	}

	ctx := context.Background()
	checker := NewBucketCheckerCoalesced(&BucketCheckerCoalescedOptions{
		Checker: baseChecker,
	})

	testReqCh := make(chan string, 128)
	testRespCh := make(chan *bucketCheckResult, 128)

	// start 4 threads to process requests
	for i := 0; i < 4; i++ {
		go func() {
			for req := range testReqCh {
				log.Printf("Calling HasBucket")
				res, err := checker.HasBucket(ctx, req)
				log.Printf("Called HasBucket")
				testRespCh <- &bucketCheckResult{Result: res, Error: err}
			}
		}()
	}

	// send the first request
	testReqCh <- "test1"

	// wait for the check to start with the mock
	<-checkSigCh

	// send 2 more requests that should be coalesced together
	testReqCh <- "test1"
	testReqCh <- "test1"

	// need to wait 10 millis for those requests to make it to the coalescing
	time.Sleep(10 * time.Millisecond)

	// send the result for the first check
	resultCh <- &bucketCheckResult{Result: true, Error: nil}

	// check that our first request returns true as expected
	resp := <-testRespCh
	assert.NoError(t, resp.Error)
	assert.Equal(t, true, resp.Result)

	// send the result for the second two checks
	resultCh <- &bucketCheckResult{Result: false, Error: nil}

	resp = <-testRespCh
	assert.NoError(t, resp.Error)
	assert.Equal(t, false, resp.Result)

	resp = <-testRespCh
	assert.NoError(t, resp.Error)
	assert.Equal(t, false, resp.Result)

	// kill the goroutines
	close(testReqCh)
}
