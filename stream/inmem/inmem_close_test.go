package inmem_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enverbisevac/libs/stream"
	"github.com/enverbisevac/libs/stream/inmem"
	"github.com/stretchr/testify/require"
)

func TestServiceClose_StopsAllConsumers(t *testing.T) {
	svc := inmem.New()

	ctx := context.Background()
	var calls atomic.Int32

	_, err := svc.Subscribe(ctx, "a", "g",
		func(_ context.Context, _ *stream.Message) error {
			calls.Add(1)
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(2*time.Millisecond),
	)
	require.NoError(t, err)

	_, err = svc.Subscribe(ctx, "b", "g",
		func(_ context.Context, _ *stream.Message) error {
			calls.Add(1)
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(2*time.Millisecond),
	)
	require.NoError(t, err)

	require.NoError(t, svc.Publish(ctx, "a", []byte("x")))
	require.NoError(t, svc.Publish(ctx, "b", []byte("x")))

	require.Eventually(t, func() bool {
		return calls.Load() == 2
	}, 2*time.Second, 5*time.Millisecond)

	require.NoError(t, svc.Close(ctx))

	// After Close, Publish must error and Subscribe must error.
	require.ErrorIs(t, svc.Publish(ctx, "a", []byte("y")), inmem.ErrClosed)
	_, err = svc.Subscribe(ctx, "a", "g2",
		func(_ context.Context, _ *stream.Message) error { return nil })
	require.ErrorIs(t, err, inmem.ErrClosed)

	// Idempotent.
	require.NoError(t, svc.Close(ctx))
}
