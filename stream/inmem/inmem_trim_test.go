package inmem_test

import (
	"context"
	"testing"
	"time"

	"github.com/enverbisevac/libs/stream"
	"github.com/enverbisevac/libs/stream/inmem"
	"github.com/stretchr/testify/require"
)

func TestPublish_TrimByMaxLen(t *testing.T) {
	svc := inmem.New()
	defer func() { _ = svc.Close(context.Background()) }()

	ctx := context.Background()
	formatted := stream.FormatStream(stream.DefaultAppName, stream.DefaultNamespace, "orders")

	for range 10 {
		require.NoError(t, svc.Publish(ctx, "orders", []byte("x"),
			stream.WithMaxLen(3)))
	}
	require.Equal(t, 3, svc.LogLen(formatted))
}

func TestPublish_TrimByMaxAge(t *testing.T) {
	svc := inmem.New()
	defer func() { _ = svc.Close(context.Background()) }()

	ctx := context.Background()
	formatted := stream.FormatStream(stream.DefaultAppName, stream.DefaultNamespace, "orders")

	require.NoError(t, svc.Publish(ctx, "orders", []byte("old")))
	time.Sleep(60 * time.Millisecond)
	// Now publish with MaxAge=50ms; the prior message is older than that.
	require.NoError(t, svc.Publish(ctx, "orders", []byte("new"),
		stream.WithMaxAge(50*time.Millisecond)))

	require.Equal(t, 1, svc.LogLen(formatted), "old message past MaxAge must be trimmed")
}

func TestPublish_TrimZeroMeansNoTrim(t *testing.T) {
	svc := inmem.New()
	defer func() { _ = svc.Close(context.Background()) }()

	ctx := context.Background()
	formatted := stream.FormatStream(stream.DefaultAppName, stream.DefaultNamespace, "orders")

	for range 5 {
		require.NoError(t, svc.Publish(ctx, "orders", []byte("x")))
	}
	require.Equal(t, 5, svc.LogLen(formatted))
}
