package streamtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enverbisevac/libs/stream"
	"github.com/stretchr/testify/require"
)

// Factory produces a fresh stream.Service per sub-test along with a
// teardown function. The factory is responsible for using a unique
// namespace per call so durable backends do not bleed state between
// sub-tests.
type Factory func(t *testing.T) (svc stream.Service, teardown func())

// uniqueName returns a stream name that won't collide between sub-tests.
func uniqueName(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// waitFor polls f every 5ms until it returns true or timeout elapses.
// Used instead of sleeps so tests are robust to backend wake-up jitter.
func waitFor(t *testing.T, timeout time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v", timeout)
}

// Run executes the conformance suite against a backend.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("Roundtrip", func(t *testing.T) { testRoundtrip(t, factory) })
	t.Run("ConsumerGroupFanIn", func(t *testing.T) { testFanIn(t, factory) })
	t.Run("TwoGroupsSameStream", func(t *testing.T) { testTwoGroups(t, factory) })
	t.Run("StartFromLatest", func(t *testing.T) { testStartLatest(t, factory) })
	t.Run("StartFromEarliest", func(t *testing.T) { testStartEarliest(t, factory) })
	t.Run("StartFromID", func(t *testing.T) { testStartFromID(t, factory) })
	t.Run("VisibilityTimeoutReclaim", func(t *testing.T) { testVisibilityReclaim(t, factory) })
	t.Run("RetryThenDLQ", func(t *testing.T) { testRetryThenDLQ(t, factory) })
	t.Run("SkipRetryRoutesToDLQ", func(t *testing.T) { testSkipRetry(t, factory) })
	t.Run("HeadersRoundTrip", func(t *testing.T) { testHeaders(t, factory) })
	t.Run("TrimByMaxLen", func(t *testing.T) { testTrimMaxLen(t, factory) })
	t.Run("TrimByMaxAge", func(t *testing.T) { testTrimMaxAge(t, factory) })
	t.Run("ConcurrencyNoDoubleDelivery", func(t *testing.T) { testConcurrency(t, factory) })
	t.Run("SubscribeCtxIndependentFromConsumer", func(t *testing.T) { testSubscribeCtxIndependence(t, factory) })
}

// All test functions are stubs that call t.Skip with the implementing-task
// name. T18-T25 replace these with real implementations.

func testRoundtrip(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "roundtrip")

	const n = 5
	type got struct {
		mu  sync.Mutex
		all []string
	}
	g := &got{}

	cons, err := svc.Subscribe(ctx, streamName, "g1",
		func(_ context.Context, m *stream.Message) error {
			g.mu.Lock()
			g.all = append(g.all, string(m.Payload))
			g.mu.Unlock()
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	for i := range n {
		require.NoError(t, svc.Publish(ctx, streamName, fmt.Appendf(nil, "m%d", i)))
	}

	waitFor(t, 15*time.Second, func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return len(g.all) == n
	})

	g.mu.Lock()
	defer g.mu.Unlock()
	require.Len(t, g.all, n)
}

func testFanIn(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "fanin")

	const n = 100
	var (
		mu  sync.Mutex
		ids = make(map[string]struct{})
		dup int
	)

	cons, err := svc.Subscribe(ctx, streamName, "shared",
		func(_ context.Context, m *stream.Message) error {
			mu.Lock()
			defer mu.Unlock()
			if _, exists := ids[m.ID]; exists {
				dup++
			} else {
				ids[m.ID] = struct{}{}
			}
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithConcurrency(3),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	for range n {
		require.NoError(t, svc.Publish(ctx, streamName, []byte("x")))
	}

	waitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(ids) == n
	})

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 0, dup, "no message should be delivered twice within a single group")
}

func testTwoGroups(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "twogroups")

	const n = 20
	var (
		mu       sync.Mutex
		gotByGrp = map[string]int{"a": 0, "b": 0}
	)

	for _, group := range []string{"a", "b"} {
		cons, err := svc.Subscribe(ctx, streamName, group,
			func(_ context.Context, _ *stream.Message) error {
				mu.Lock()
				gotByGrp[group]++
				mu.Unlock()
				return nil
			},
			stream.WithStartFrom(stream.StartEarliest),
			stream.WithPollInterval(5*time.Millisecond),
		)
		require.NoError(t, err)
		defer func() { _ = cons.Close() }()
	}

	for range n {
		require.NoError(t, svc.Publish(ctx, streamName, []byte("x")))
	}

	waitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotByGrp["a"] == n && gotByGrp["b"] == n
	})

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, n, gotByGrp["a"])
	require.Equal(t, n, gotByGrp["b"])
}

func testStartLatest(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "latest")

	// Pre-publish 3 messages BEFORE creating the group.
	for range 3 {
		require.NoError(t, svc.Publish(ctx, streamName, []byte("pre")))
	}

	var seen []string
	var mu sync.Mutex
	cons, err := svc.Subscribe(ctx, streamName, "latest-grp",
		func(_ context.Context, m *stream.Message) error {
			mu.Lock()
			seen = append(seen, string(m.Payload))
			mu.Unlock()
			return nil
		},
		// StartLatest is the default but explicit here for clarity.
		stream.WithStartFrom(stream.StartLatest),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	// Give the worker time to register at "latest" before publishing
	// post-creation messages.
	time.Sleep(50 * time.Millisecond)

	for range 2 {
		require.NoError(t, svc.Publish(ctx, streamName, []byte("post")))
	}

	waitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 2
	})

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"post", "post"}, seen, "StartLatest must skip backlog")
}

func testStartEarliest(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "earliest")

	for range 3 {
		require.NoError(t, svc.Publish(ctx, streamName, []byte("pre")))
	}

	var got int
	var mu sync.Mutex
	cons, err := svc.Subscribe(ctx, streamName, "earliest-grp",
		func(_ context.Context, _ *stream.Message) error {
			mu.Lock()
			got++
			mu.Unlock()
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	waitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got == 3
	})
}

func testStartFromID(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "startid")

	// First, capture some IDs by subscribing eagerly with a recording group.
	var (
		recMu  sync.Mutex
		recIDs []string
	)
	recCons, err := svc.Subscribe(ctx, streamName, "recorder",
		func(_ context.Context, m *stream.Message) error {
			recMu.Lock()
			recIDs = append(recIDs, m.ID)
			recMu.Unlock()
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)

	for i := range 5 {
		require.NoError(t, svc.Publish(ctx, streamName, fmt.Appendf(nil, "m%d", i)))
	}

	waitFor(t, 15*time.Second, func() bool {
		recMu.Lock()
		defer recMu.Unlock()
		return len(recIDs) == 5
	})
	_ = recCons.Close()

	// Now subscribe with a fresh group starting strictly after recIDs[1].
	// Expect to see the 3 messages with seq > recIDs[1].
	startID := recIDs[1]

	var (
		gotMu sync.Mutex
		got   []string
	)
	cons, err := svc.Subscribe(ctx, streamName, "after-grp",
		func(_ context.Context, m *stream.Message) error {
			gotMu.Lock()
			got = append(got, string(m.Payload))
			gotMu.Unlock()
			return nil
		},
		stream.WithStartFromID(startID),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	waitFor(t, 15*time.Second, func() bool {
		gotMu.Lock()
		defer gotMu.Unlock()
		return len(got) == 3
	})
}

func testVisibilityReclaim(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "reclaim")

	var deliveries atomic.Int32
	hangCh := make(chan struct{})

	cons, err := svc.Subscribe(ctx, streamName, "r-grp",
		func(_ context.Context, m *stream.Message) error {
			n := deliveries.Add(1)
			if n == 1 {
				<-hangCh
			}
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithVisibilityTimeout(80*time.Millisecond),
		stream.WithPollInterval(10*time.Millisecond),
		stream.WithConcurrency(2),
	)
	require.NoError(t, err)
	defer func() {
		// Close hangCh BEFORE cons.Close() so the hung handler returns
		// and cons.Close() doesn't block for ShutdownTimeout.
		close(hangCh)
		_ = cons.Close()
	}()

	require.NoError(t, svc.Publish(ctx, streamName, []byte("only-one")))

	waitFor(t, 15*time.Second, func() bool {
		return deliveries.Load() >= 2
	})
}

func testRetryThenDLQ(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "dlq")

	var attempts atomic.Int32
	cons, err := svc.Subscribe(ctx, streamName, "fail",
		func(_ context.Context, _ *stream.Message) error {
			attempts.Add(1)
			return errors.New("boom")
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithMaxRetries(2), // 3 total attempts before DLQ
		stream.WithVisibilityTimeout(30*time.Millisecond),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	var dlqCount atomic.Int32
	dlqCons, err := svc.Subscribe(ctx, streamName+".dead", "alerter",
		func(_ context.Context, _ *stream.Message) error {
			dlqCount.Add(1)
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = dlqCons.Close() }()

	require.NoError(t, svc.Publish(ctx, streamName, []byte("poison")))

	waitFor(t, 15*time.Second, func() bool {
		return dlqCount.Load() == 1
	})

	require.GreaterOrEqual(t, int(attempts.Load()), 3)
}

func testSkipRetry(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "skipretry")

	var attempts atomic.Int32
	cons, err := svc.Subscribe(ctx, streamName, "fail",
		func(_ context.Context, _ *stream.Message) error {
			attempts.Add(1)
			return stream.SkipRetry(errors.New("permanent"))
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithMaxRetries(99),
		stream.WithVisibilityTimeout(30*time.Millisecond),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	var dlqCount atomic.Int32
	dlqCons, err := svc.Subscribe(ctx, streamName+".dead", "alerter",
		func(_ context.Context, _ *stream.Message) error {
			dlqCount.Add(1)
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = dlqCons.Close() }()

	require.NoError(t, svc.Publish(ctx, streamName, []byte("permanent")))

	waitFor(t, 15*time.Second, func() bool {
		return dlqCount.Load() == 1
	})

	require.EqualValues(t, 1, attempts.Load(), "SkipRetry must route on first attempt")
}

func testHeaders(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "headers")

	var (
		mu      sync.Mutex
		gotHdrs map[string]string
	)
	cons, err := svc.Subscribe(ctx, streamName, "g",
		func(_ context.Context, m *stream.Message) error {
			mu.Lock()
			gotHdrs = m.Headers
			mu.Unlock()
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	require.NoError(t, svc.Publish(ctx, streamName, []byte("p"),
		stream.WithHeaders(map[string]string{
			"trace-id":     "abc",
			"content-type": "application/json",
		})))

	waitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotHdrs != nil
	})

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "abc", gotHdrs["trace-id"])
	require.Equal(t, "application/json", gotHdrs["content-type"])
}

func testTrimMaxLen(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "trimlen")

	const total = 20
	const keep = 5

	for i := range total {
		require.NoError(t, svc.Publish(ctx, streamName,
			fmt.Appendf(nil, "m%d", i),
			stream.WithMaxLen(keep)))
	}

	var (
		mu   sync.Mutex
		seen []string
	)
	cons, err := svc.Subscribe(ctx, streamName, "after-trim",
		func(_ context.Context, m *stream.Message) error {
			mu.Lock()
			seen = append(seen, string(m.Payload))
			mu.Unlock()
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	// Must see exactly the last `keep` payloads. Allow a small grace to
	// confirm no straggler arrives.
	waitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= keep
	})
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, keep, "Earliest subscriber must see only the un-trimmed window")
	for i, p := range seen {
		want := fmt.Sprintf("m%d", total-keep+i)
		require.Equal(t, want, p)
	}
}

func testTrimMaxAge(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "trimage")

	require.NoError(t, svc.Publish(ctx, streamName, []byte("old")))
	time.Sleep(120 * time.Millisecond)
	require.NoError(t, svc.Publish(ctx, streamName, []byte("new"),
		stream.WithMaxAge(80*time.Millisecond)))

	var (
		mu  sync.Mutex
		got []string
	)
	cons, err := svc.Subscribe(ctx, streamName, "after-age-trim",
		func(_ context.Context, m *stream.Message) error {
			mu.Lock()
			got = append(got, string(m.Payload))
			mu.Unlock()
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	waitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) >= 1
	})
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"new"}, got, "messages older than MaxAge must be trimmed")
}

func testConcurrency(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	ctx := t.Context()
	streamName := uniqueName(t, "concurrent")

	const total = 200
	var (
		mu  sync.Mutex
		ids = make(map[string]struct{})
		dup int
	)

	cons, err := svc.Subscribe(ctx, streamName, "shared",
		func(_ context.Context, m *stream.Message) error {
			mu.Lock()
			defer mu.Unlock()
			if _, exists := ids[m.ID]; exists {
				dup++
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

	// Publish concurrently from multiple goroutines.
	var pubWg sync.WaitGroup
	for range 4 {
		pubWg.Go(func() {
			for range total / 4 {
				require.NoError(t, svc.Publish(ctx, streamName, []byte("x")))
			}
		})
	}
	pubWg.Wait()

	// 30s budget is lenient enough to absorb pgx cold-start (pool warm-up
	// + listener acquire + schema apply on first invocation). Inmem,
	// sqlite, and redis routinely finish this case in well under 2s.
	waitFor(t, 30*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(ids) == total
	})

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 0, dup)
}

// testSubscribeCtxIndependence asserts that canceling the ctx passed to
// Subscribe does not stop the consumer. Worker lifetime is owned by
// Consumer.Close — request-scoped contexts must not implicitly tear down
// long-running consumers.
func testSubscribeCtxIndependence(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()

	streamName := uniqueName(t, "ctx_indep")

	var received atomic.Int32
	subCtx, cancelSub := context.WithCancel(context.Background())
	cons, err := svc.Subscribe(subCtx, streamName, "g1",
		func(_ context.Context, _ *stream.Message) error {
			received.Add(1)
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	// Cancel the Subscribe ctx; the consumer must keep running.
	cancelSub()

	// Give backends a beat to notice (or not) the cancellation.
	time.Sleep(100 * time.Millisecond)

	// Publish after cancellation — handler should still receive it.
	require.NoError(t, svc.Publish(context.Background(), streamName, []byte("after-cancel")))

	waitFor(t, 5*time.Second, func() bool {
		return received.Load() >= 1
	})
}
