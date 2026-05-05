package inmem_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enverbisevac/libs/stream"
	"github.com/enverbisevac/libs/stream/inmem"
	"github.com/stretchr/testify/require"
)

func TestRetryThenDLQ(t *testing.T) {
	svc := inmem.New()
	defer func() { _ = svc.Close(context.Background()) }()

	ctx := context.Background()

	var attempts atomic.Int32
	cons, err := svc.Subscribe(ctx, "orders", "billing",
		func(_ context.Context, m *stream.Message) error {
			attempts.Add(1)
			return errors.New("boom")
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithMaxRetries(2), // 3 total attempts before DLQ
		stream.WithVisibilityTimeout(20*time.Millisecond),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	// Subscribe to the DLQ to confirm routing.
	var (
		dlqMu  sync.Mutex
		dlqIDs []string
	)
	dlqCons, err := svc.Subscribe(ctx, "orders.dead", "alerter",
		func(_ context.Context, m *stream.Message) error {
			dlqMu.Lock()
			dlqIDs = append(dlqIDs, m.ID)
			dlqMu.Unlock()
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = dlqCons.Close() }()

	require.NoError(t, svc.Publish(ctx, "orders", []byte("poison")))

	require.Eventually(t, func() bool {
		dlqMu.Lock()
		defer dlqMu.Unlock()
		return len(dlqIDs) == 1
	}, 2*time.Second, 10*time.Millisecond, "message never reached DLQ")

	// Allow a small settling window after DLQ arrival to ensure no extra
	// reclaim cycle bumps attempts further.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(3), attempts.Load(), "MaxRetries=2 must yield exactly 3 total attempts before DLQ")
}

func TestSkipRetryRoutesToDLQImmediately(t *testing.T) {
	svc := inmem.New()
	defer func() { _ = svc.Close(context.Background()) }()

	ctx := context.Background()

	var attempts atomic.Int32
	cons, err := svc.Subscribe(ctx, "orders", "billing",
		func(_ context.Context, m *stream.Message) error {
			attempts.Add(1)
			return stream.SkipRetry(errors.New("permanent"))
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithMaxRetries(99),
		stream.WithVisibilityTimeout(20*time.Millisecond),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	var (
		dlqMu sync.Mutex
		dlqs  int
	)
	dlqCons, err := svc.Subscribe(ctx, "orders.dead", "alerter",
		func(_ context.Context, m *stream.Message) error {
			dlqMu.Lock()
			dlqs++
			dlqMu.Unlock()
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = dlqCons.Close() }()

	require.NoError(t, svc.Publish(ctx, "orders", []byte("permanent-fail")))

	require.Eventually(t, func() bool {
		dlqMu.Lock()
		defer dlqMu.Unlock()
		return dlqs == 1
	}, 2*time.Second, 10*time.Millisecond)

	require.Equal(t, int32(1), attempts.Load(), "SkipRetry must route to DLQ on first attempt")
}
