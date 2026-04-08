package queue_test

import (
	"errors"
	"testing"
	"time"

	"github.com/enverbisevac/libs/queue"
)

func TestPriorityConstants(t *testing.T) {
	if queue.PriorityLow >= queue.PriorityNormal {
		t.Fatalf("PriorityLow should be less than PriorityNormal")
	}
	if queue.PriorityNormal >= queue.PriorityHigh {
		t.Fatalf("PriorityNormal should be less than PriorityHigh")
	}
}

func TestErrSkipRetry(t *testing.T) {
	wrapped := errors.New("bad payload")
	err := queue.SkipRetry(wrapped)
	if !errors.Is(err, queue.ErrSkipRetry) {
		t.Fatalf("SkipRetry result should match ErrSkipRetry")
	}
	if !errors.Is(err, wrapped) {
		t.Fatalf("SkipRetry result should still wrap the original error")
	}
}

func TestJobZeroValue(t *testing.T) {
	var j queue.Job
	if j.Attempt != 0 {
		t.Fatalf("zero job should have Attempt 0")
	}
	if !j.EnqueuedAt.IsZero() {
		t.Fatalf("zero job should have zero EnqueuedAt")
	}
	_ = time.Now() // keep import
}
