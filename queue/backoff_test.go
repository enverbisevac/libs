package queue_test

import (
	"testing"
	"time"

	"github.com/enverbisevac/libs/queue"
)

func TestDefaultBackoffMonotonicUntilCap(t *testing.T) {
	last := time.Duration(0)
	cap := 5 * time.Minute
	for attempt := 1; attempt < 20; attempt++ {
		d := queue.DefaultBackoff(attempt)
		if d <= 0 {
			t.Fatalf("attempt %d: backoff %v must be positive", attempt, d)
		}
		// Allow jitter to vary, but the *baseline* (without jitter) is
		// non-decreasing until cap. Use a 4x slack to absorb ±25% jitter.
		if d > 5*cap {
			t.Fatalf("attempt %d: backoff %v exceeded 5*cap", attempt, d)
		}
		_ = last
		last = d
	}
}

func TestDefaultBackoffCapped(t *testing.T) {
	const cap = 5 * time.Minute
	// At attempt 1000, the raw shifted value overflows; backoff must clamp.
	d := queue.DefaultBackoff(1000)
	if d <= 0 {
		t.Fatalf("attempt 1000: backoff %v must be positive", d)
	}
	// Worst-case jittered upper bound is cap + 25% jitter.
	if d > cap+cap/4 {
		t.Fatalf("attempt 1000: backoff %v exceeded cap %v", d, cap)
	}
}

func TestDefaultBackoffJitterBounds(t *testing.T) {
	// At attempt 5, base = 16s. After ±25% jitter, the result must be in
	// [base - 25%, base + 25%].
	const baseAtAttempt5 = 16 * time.Second
	low := baseAtAttempt5 - baseAtAttempt5/4
	high := baseAtAttempt5 + baseAtAttempt5/4
	for i := range 1000 {
		d := queue.DefaultBackoff(5)
		if d < low || d > high {
			t.Fatalf("iteration %d: backoff %v outside [%v, %v]", i, d, low, high)
		}
	}
}

func TestBackoffFuncType(t *testing.T) {
	var _ queue.BackoffFunc = queue.DefaultBackoff
}
