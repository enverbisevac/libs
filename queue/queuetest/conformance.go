package queuetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enverbisevac/libs/queue"
	"github.com/stretchr/testify/require"
)

// Factory produces a fresh queue.Service per sub-test along with a teardown
// function. The factory is responsible for using a unique namespace per call
// so durable backends do not bleed state between sub-tests.
type Factory func(t *testing.T) (svc queue.Service, teardown func())

// Run executes the conformance suite against a backend.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("FIFO", func(t *testing.T) { testFIFO(t, factory) })
	t.Run("Priority", func(t *testing.T) { testPriority(t, factory) })
	t.Run("Delay", func(t *testing.T) { testDelay(t, factory) })
	t.Run("RetryThenSuccess", func(t *testing.T) { testRetryThenSuccess(t, factory) })
	t.Run("RetryExhaustedRoutesToDLQ", func(t *testing.T) { testDLQ(t, factory) })
	t.Run("SkipRetryRoutesToDLQ", func(t *testing.T) { testSkipRetry(t, factory) })
	t.Run("PanicRecovery", func(t *testing.T) { testPanicRecovery(t, factory) })
	t.Run("ConcurrencyNoDoubleProcessing", func(t *testing.T) { testConcurrency(t, factory) })
	t.Run("VisibilityTimeoutReclaim", func(t *testing.T) { testVisibilityReclaim(t, factory) })
	t.Run("GracefulShutdownDrains", func(t *testing.T) { testGracefulShutdown(t, factory) })
}

// uniqueTopic returns a topic name that won't collide between sub-tests.
func uniqueTopic(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// waitFor polls f every 10ms until it returns true or timeout elapses. Used
// instead of sleeps so tests are robust to backend wake-up jitter.
func waitFor(t *testing.T, timeout time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v", timeout)
}

func testFIFO(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()
	topic := uniqueTopic(t, "fifo")
	ctx := context.Background()

	const n = 10
	var (
		mu   sync.Mutex
		seen []string
	)
	cons, err := svc.Subscribe(ctx, topic, func(_ context.Context, j *queue.Job) error {
		mu.Lock()
		seen = append(seen, string(j.Payload))
		mu.Unlock()
		return nil
	}, queue.WithConcurrency(1))
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	for i := range n {
		require.NoError(t, svc.Enqueue(ctx, topic, fmt.Appendf(nil, "%d", i)))
	}

	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == n
	})
	mu.Lock()
	defer mu.Unlock()
	for i, v := range seen {
		want := fmt.Sprintf("%d", i)
		if v != want {
			t.Fatalf("position %d: got %q, want %q", i, v, want)
		}
	}
}

func testPriority(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()
	topic := uniqueTopic(t, "prio")
	ctx := context.Background()

	// Enqueue with consumer NOT yet running so all jobs are simultaneously
	// ready when the consumer starts. This isolates ordering from arrival
	// timing.
	require.NoError(t, svc.Enqueue(ctx, topic, []byte("low"), queue.WithPriority(queue.PriorityLow)))
	require.NoError(t, svc.Enqueue(ctx, topic, []byte("normal")))
	require.NoError(t, svc.Enqueue(ctx, topic, []byte("high"), queue.WithPriority(queue.PriorityHigh)))

	var (
		mu   sync.Mutex
		seen []string
	)
	cons, err := svc.Subscribe(ctx, topic, func(_ context.Context, j *queue.Job) error {
		mu.Lock()
		seen = append(seen, string(j.Payload))
		mu.Unlock()
		return nil
	}, queue.WithConcurrency(1))
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 3
	})
	mu.Lock()
	defer mu.Unlock()
	want := []string{"high", "normal", "low"}
	for i, v := range seen {
		if v != want[i] {
			t.Fatalf("position %d: got %q, want %q (full: %v)", i, v, want[i], seen)
		}
	}
}

func testDelay(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()
	topic := uniqueTopic(t, "delay")
	ctx := context.Background()

	var received atomic.Int64
	cons, err := svc.Subscribe(ctx, topic, func(_ context.Context, _ *queue.Job) error {
		received.Store(time.Now().UnixNano())
		return nil
	})
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	enqueueAt := time.Now()
	require.NoError(t, svc.Enqueue(ctx, topic, []byte("d"), queue.WithDelay(500*time.Millisecond)))

	waitFor(t, 5*time.Second, func() bool { return received.Load() != 0 })
	gotAt := time.Unix(0, received.Load())
	if gotAt.Sub(enqueueAt) < 400*time.Millisecond {
		t.Fatalf("delivered too early: %v after enqueue", gotAt.Sub(enqueueAt))
	}
}

func testRetryThenSuccess(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()
	topic := uniqueTopic(t, "retry")
	ctx := context.Background()

	var attempts atomic.Int64
	done := make(chan struct{})
	cons, err := svc.Subscribe(ctx, topic, func(_ context.Context, _ *queue.Job) error {
		n := attempts.Add(1)
		if n < 3 {
			return fmt.Errorf("attempt %d", n)
		}
		close(done)
		return nil
	}, queue.WithMaxRetries(5), queue.WithBackoff(func(int) time.Duration { return 50 * time.Millisecond }))
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	require.NoError(t, svc.Enqueue(ctx, topic, []byte("x")))
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("never succeeded after retries; attempts=%d", attempts.Load())
	}
}

func testDLQ(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()
	topic := uniqueTopic(t, "dlq")
	ctx := context.Background()

	failures := make(chan struct{}, 16)
	cons, err := svc.Subscribe(ctx, topic, func(_ context.Context, _ *queue.Job) error {
		failures <- struct{}{}
		return errors.New("always fail")
	}, queue.WithMaxRetries(2), queue.WithBackoff(func(int) time.Duration { return 20 * time.Millisecond }))
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	dlqGot := make(chan []byte, 1)
	// The DLQ topic the conformance test subscribes to is the user-facing
	// short name with ".dead" appended; backends MUST resolve the dead letter
	// topic with the same formatting that Enqueue uses, so subscribing to
	// "<topic>.dead" with the same service instance picks up the routed jobs.
	dlqCons, err := svc.Subscribe(ctx, topic+".dead",
		func(_ context.Context, j *queue.Job) error {
			dlqGot <- j.Payload
			return nil
		},
	)
	require.NoError(t, err)
	defer func() { _ = dlqCons.Close() }()

	require.NoError(t, svc.Enqueue(ctx, topic, []byte("dead-payload")))

	// Expect: 1 initial + 2 retries = 3 attempts before DLQ.
	for i := range 3 {
		select {
		case <-failures:
		case <-time.After(5 * time.Second):
			t.Fatalf("missing failure %d", i+1)
		}
	}
	select {
	case got := <-dlqGot:
		if string(got) != "dead-payload" {
			t.Fatalf("DLQ payload = %q", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("DLQ never received the job")
	}
}

func testSkipRetry(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()
	topic := uniqueTopic(t, "skipretry")
	ctx := context.Background()

	var attempts atomic.Int64
	cons, err := svc.Subscribe(ctx, topic, func(_ context.Context, _ *queue.Job) error {
		attempts.Add(1)
		return queue.SkipRetry(errors.New("permanent"))
	}, queue.WithMaxRetries(10))
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	dlqGot := make(chan struct{}, 1)
	dlqCons, err := svc.Subscribe(ctx, topic+".dead",
		func(_ context.Context, _ *queue.Job) error {
			dlqGot <- struct{}{}
			return nil
		},
	)
	require.NoError(t, err)
	defer func() { _ = dlqCons.Close() }()

	require.NoError(t, svc.Enqueue(ctx, topic, []byte("p")))

	select {
	case <-dlqGot:
	case <-time.After(5 * time.Second):
		t.Fatalf("DLQ never received skip-retry job; attempts=%d", attempts.Load())
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("expected exactly 1 attempt for skip-retry, got %d", got)
	}
}

func testPanicRecovery(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()
	topic := uniqueTopic(t, "panic")
	ctx := context.Background()

	var attempts atomic.Int64
	done := make(chan struct{})
	cons, err := svc.Subscribe(ctx, topic, func(_ context.Context, _ *queue.Job) error {
		n := attempts.Add(1)
		if n == 1 {
			panic("boom")
		}
		close(done)
		return nil
	}, queue.WithMaxRetries(3), queue.WithBackoff(func(int) time.Duration { return 20 * time.Millisecond }))
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	require.NoError(t, svc.Enqueue(ctx, topic, []byte("p")))
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("worker did not recover from panic; attempts=%d", attempts.Load())
	}
}

func testConcurrency(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()
	topic := uniqueTopic(t, "conc")
	ctx := context.Background()

	const n = 50
	var (
		mu   sync.Mutex
		seen = map[string]int{}
	)
	cons, err := svc.Subscribe(ctx, topic, func(_ context.Context, j *queue.Job) error {
		mu.Lock()
		seen[string(j.Payload)]++
		mu.Unlock()
		return nil
	}, queue.WithConcurrency(8))
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	for i := range n {
		require.NoError(t, svc.Enqueue(ctx, topic, fmt.Appendf(nil, "%d", i)))
	}

	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == n
	})
	mu.Lock()
	defer mu.Unlock()
	for k, v := range seen {
		if v != 1 {
			t.Fatalf("job %q processed %d times, want 1", k, v)
		}
	}
}

func testVisibilityReclaim(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()
	topic := uniqueTopic(t, "vis")
	ctx := context.Background()

	var attempts atomic.Int64
	done := make(chan struct{})
	// First handler hangs; second handler completes after re-claim.
	cons, err := svc.Subscribe(ctx, topic, func(_ context.Context, _ *queue.Job) error {
		n := attempts.Add(1)
		if n == 1 {
			// Sleep longer than the visibility timeout. The second worker
			// (or this same goroutine on next poll) will re-claim.
			time.Sleep(2 * time.Second)
			return errors.New("simulated stuck")
		}
		close(done)
		return nil
	},
		queue.WithMaxRetries(5),
		queue.WithVisibilityTimeout(500*time.Millisecond),
		queue.WithBackoff(func(int) time.Duration { return 50 * time.Millisecond }),
		queue.WithConcurrency(2),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	require.NoError(t, svc.Enqueue(ctx, topic, []byte("v")))
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("re-claim never happened; attempts=%d", attempts.Load())
	}
}

func testGracefulShutdown(t *testing.T, factory Factory) {
	svc, td := factory(t)
	defer td()
	topic := uniqueTopic(t, "shutdown")
	ctx := context.Background()

	started := make(chan struct{})
	finished := make(chan struct{})
	cons, err := svc.Subscribe(ctx, topic, func(_ context.Context, _ *queue.Job) error {
		close(started)
		time.Sleep(200 * time.Millisecond)
		close(finished)
		return nil
	}, queue.WithShutdownTimeout(2*time.Second))
	require.NoError(t, err)

	require.NoError(t, svc.Enqueue(ctx, topic, []byte("s")))
	<-started
	require.NoError(t, cons.Close())
	select {
	case <-finished:
	default:
		t.Fatalf("Close returned before in-flight handler finished")
	}
}
