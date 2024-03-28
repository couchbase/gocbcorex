package gocbcorex_test

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/contrib/leakcheck"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
)

func TestAgentDelete(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	opts := gocbcorex.AgentOptions{
		Logger:    logger,
		TLSConfig: nil,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
		BucketName: testutilsint.TestOpts.BucketName,
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: testutilsint.TestOpts.HTTPAddrs,
			MemdAddrs: testutilsint.TestOpts.MemdAddrs,
		},
	}

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
		require.False(t, leakcheck.ReportLeakedGoroutines())
	})

	docKey := uuid.NewString()

	upsertRes, err := agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	deleteRes, err := agent.Delete(context.Background(), &gocbcorex.DeleteOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)
	assert.NotZero(t, deleteRes.Cas)

	_, err = agent.Get(context.Background(), &gocbcorex.GetOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
	})
	require.ErrorIs(t, err, memdx.ErrDocNotFound)
}

func TestAgentDoesNotRetryMemdxInvalidArgs(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	opts := gocbcorex.AgentOptions{
		Logger:    logger,
		TLSConfig: nil,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
		BucketName: testutilsint.TestOpts.BucketName,
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: testutilsint.TestOpts.HTTPAddrs,
			MemdAddrs: testutilsint.TestOpts.MemdAddrs,
		},
	}

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
		require.False(t, leakcheck.ReportLeakedGoroutines())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err = agent.RangeScanCreate(ctx, &gocbcorex.RangeScanCreateOptions{
		VbucketID: 1,
	})
	require.ErrorIs(t, err, memdx.ErrInvalidArgument)
}

func TestServerDurations(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	opts := gocbcorex.AgentOptions{
		Logger:    logger,
		TLSConfig: nil,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
		BucketName: testutilsint.TestOpts.BucketName,
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: testutilsint.TestOpts.HTTPAddrs,
			MemdAddrs: testutilsint.TestOpts.MemdAddrs,
		},
	}

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
		require.False(t, leakcheck.ReportLeakedGoroutines())
	})

	docKey := uuid.NewString()

	ctx := context.Background()

	memExporter := tracetest.NewInMemoryExporter()
	memTracer := trace.NewTracerProvider(
		trace.WithSyncer(memExporter),
	)
	otel.SetTracerProvider(memTracer)

	tracer := otel.Tracer("github.com/couchbase/gocbcorex.tests")
	spanCtx, span := tracer.Start(ctx, "testGet")

	memExporter.Reset()

	// do an upsert, and then verify the server duration is included
	upsertRes, err := agent.Upsert(spanCtx, &gocbcorex.UpsertOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	spans := memExporter.GetSpans()
	spansSnap := spans.Snapshots()

	foundServerDuration := false
	for _, spanSnap := range spansSnap {
		attribs := spanSnap.Attributes()
		for _, attrib := range attribs {
			if attrib.Key == "db.couchbase.server_duration" {
				foundServerDuration = true
			}
		}
	}
	require.True(t, foundServerDuration)

	// cleanup the document we created
	deleteRes, err := agent.Delete(spanCtx, &gocbcorex.DeleteOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)
	assert.NotZero(t, deleteRes.Cas)

	// try to fetch the document again to trigger an error and confirm
	// that we don't fail.  This is a regression test for ING-781.
	_, err = agent.Get(spanCtx, &gocbcorex.GetOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
	})
	require.ErrorIs(t, err, memdx.ErrDocNotFound)

	span.End()

	otel.SetTracerProvider(nil)
}
