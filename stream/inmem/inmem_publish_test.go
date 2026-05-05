package inmem_test

import (
	"context"
	"testing"

	"github.com/enverbisevac/libs/stream"
	"github.com/enverbisevac/libs/stream/inmem"
	"github.com/stretchr/testify/require"
)

func TestPublish_AppendsToLog(t *testing.T) {
	svc := inmem.New()
	defer func() { _ = svc.Close(context.Background()) }()

	ctx := context.Background()
	require.NoError(t, svc.Publish(ctx, "orders", []byte("a")))
	require.NoError(t, svc.Publish(ctx, "orders", []byte("b")))
	require.NoError(t, svc.Publish(ctx, "orders", []byte("c")))

	// LogLen is a test-only helper; declare on Service.
	require.Equal(t, 3, svc.LogLen(stream.FormatStream(stream.DefaultAppName, stream.DefaultNamespace, "orders")))
}

func TestPublish_AfterCloseReturnsError(t *testing.T) {
	svc := inmem.New()
	require.NoError(t, svc.Close(context.Background()))
	err := svc.Publish(context.Background(), "orders", []byte("a"))
	require.ErrorIs(t, err, inmem.ErrClosed)
}
