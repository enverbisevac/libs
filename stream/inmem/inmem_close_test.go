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

// TestCloseHonorsContext verifies Service.Close bounds an in-flight handler
// drain by the caller's ctx, not the (much larger) per-consumer
// ShutdownTimeout. Mirrors the sqlite backend's contract fix.
func TestCloseHonorsContext(t *testing.T) {
	svc := inmem.New()
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release) // let the wedged handler unwind after assertions

	_, err := svc.Subscribe(ctx, "evt", "g",
		func(_ context.Context, _ *stream.Message) error {
			close(started)
			<-release // block like a wedged handler, ignoring ctx
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(2*time.Millisecond),
		stream.WithShutdownTimeout(30*time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, svc.Publish(ctx, "evt", []byte("x")))

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never reached in-flight state")
	}

	cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = svc.Close(cctx)
	elapsed := time.Since(start)

	require.Less(t, elapsed, 5*time.Second, "Close must honor ctx, not wait the 30s ShutdownTimeout")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	t.Logf("Close returned in %v with %v (ShutdownTimeout was 30s)", elapsed.Round(time.Millisecond), err)
}
