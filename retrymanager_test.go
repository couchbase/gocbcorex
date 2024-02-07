package gocbcorex

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOrchestrateRetriesDeadlinesInOp(t *testing.T) {
	testErrMsg := "this is a message that always errors"

	retryCount := 0
	mockCtrl := &RetryControllerMock{
		ShouldRetryFunc: func(ctx context.Context, err error) (time.Duration, bool, error) {
			retryCount++
			return 0, true, nil
		},
	}
	mockMgr := &RetryManagerMock{
		NewRetryControllerFunc: func() RetryController { return mockCtrl },
	}

	// need to have enough time to call the function once at least
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Millisecond))

	fnCalls := 0
	_, err := OrchestrateRetries(ctx, mockMgr, func() (int, error) {
		fnCalls++

		// first call returns a real error
		if fnCalls == 1 {
			return 0, errors.New(testErrMsg)
		}

		// next call deadlines
		<-ctx.Done()
		return 1, ctx.Err()
	})
	cancel()

	require.Equal(t, 1, retryCount)
	require.Equal(t, 2, fnCalls)

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, testErrMsg)
}

func TestOrchestrateRetriesDeadlinesInWait(t *testing.T) {
	testErrMsg := "this is a message that always errors"

	retryCount := 0
	mockCtrl := &RetryControllerMock{
		ShouldRetryFunc: func(ctx context.Context, err error) (time.Duration, bool, error) {
			retryCount++
			return 1 * time.Second, true, nil
		},
	}
	mockMgr := &RetryManagerMock{
		NewRetryControllerFunc: func() RetryController { return mockCtrl },
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now())

	fnCalls := 0
	_, err := OrchestrateRetries(ctx, mockMgr, func() (int, error) {
		fnCalls++
		return 0, errors.New(testErrMsg)
	})
	cancel()

	require.Equal(t, 1, retryCount)
	require.Equal(t, 1, fnCalls)

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, testErrMsg)
}
