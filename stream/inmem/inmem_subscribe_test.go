package inmem_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/enverbisevac/libs/stream"
	"github.com/enverbisevac/libs/stream/inmem"
	"github.com/stretchr/testify/require"
)

func TestSubscribe_DeliversAppendedMessages(t *testing.T) {
	svc := inmem.New()
	defer func() { _ = svc.Close(context.Background()) }()

	ctx := context.Background()

	var (
		mu   sync.Mutex
		seen []string
	)
	cons, err := svc.Subscribe(ctx, "orders", "billing",
		func(_ context.Context, m *stream.Message) error {
			mu.Lock()
			seen = append(seen, string(m.Payload))
			mu.Unlock()
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	for _, p := range []string{"a", "b", "c"} {
		require.NoError(t, svc.Publish(ctx, "orders", []byte(p)))
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 3
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"a", "b", "c"}, seen)
}
