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

func TestSubscribe_VisibilityTimeoutReclaim(t *testing.T) {
	svc := inmem.New()
	defer func() { _ = svc.Close(context.Background()) }()

	ctx := context.Background()

	var deliveries atomic.Int32
	hangCh := make(chan struct{})

	cons, err := svc.Subscribe(ctx, "orders", "billing",
		func(_ context.Context, m *stream.Message) error {
			n := deliveries.Add(1)
			if n == 1 {
				// First delivery hangs past the visibility timeout to
				// simulate a stuck/crashed handler.
				<-hangCh
			}
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithVisibilityTimeout(50*time.Millisecond),
		stream.WithPollInterval(10*time.Millisecond),
		stream.WithConcurrency(2),
	)
	require.NoError(t, err)
	defer func() {
		close(hangCh)
		_ = cons.Close()
	}()

	require.NoError(t, svc.Publish(ctx, "orders", []byte("only-one")))

	// The reclaim path should re-deliver the message to the second worker
	// after VT elapses, even while the first worker is still hung.
	require.Eventually(t, func() bool {
		return deliveries.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond, "stuck pending entry was never reclaimed")
}
