package queue

import (
	"math/rand/v2"
	"time"
)

// BackoffFunc returns the delay before the given retry attempt. attempt is 1
// after the first failure, 2 after the second, and so on.
type BackoffFunc func(attempt int) time.Duration

// DefaultBackoff implements exponential backoff (1s base, 5 min cap) with
// uniform ±25% jitter. attempt overflow is handled by clamping to the cap.
func DefaultBackoff(attempt int) time.Duration {
	const (
		base = time.Second
		cap_ = 5 * time.Minute
	)
	if attempt < 1 {
		attempt = 1
	}
	// 1<<63 overflows; clamp aggressively.
	d := cap_
	if attempt < 30 {
		shifted := base << uint(attempt-1)
		if shifted > 0 && shifted < cap_ {
			d = shifted
		}
	}
	// ±25% jitter.
	jitter := time.Duration(rand.Int64N(int64(d / 2)))
	return d - d/4 + jitter
}
