package inmem_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enverbisevac/libs/stream"
	"github.com/enverbisevac/libs/stream/inmem"
	"github.com/stretchr/testify/require"
)

func TestConcurrency_NoDoubleDelivery(t *testing.T) {
	svc := inmem.New()
	defer func() { _ = svc.Close(context.Background()) }()

	ctx := context.Background()

	const total = 200

	var (
		mu  sync.Mutex
		ids = make(map[string]struct{})
		dup atomic.Int32
	)

	cons, err := svc.Subscribe(ctx, "orders", "billing",
		func(_ context.Context, m *stream.Message) error {
			mu.Lock()
			defer mu.Unlock()
			if _, exists := ids[m.ID]; exists {
				dup.Add(1)
			} else {
				ids[m.ID] = struct{}{}
			}
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithConcurrency(4),
		stream.WithPollInterval(2*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	for range total {
		require.NoError(t, svc.Publish(ctx, "orders", []byte("x")))
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(ids) == total
	}, 5*time.Second, 10*time.Millisecond)

	require.EqualValues(t, 0, dup.Load(), "no message should be delivered twice in a single group")
}
