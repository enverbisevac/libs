package queue

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Priority controls relative ordering of jobs in the same topic. Higher
// priority jobs are delivered before lower priority jobs once they are
// eligible to run.
type Priority int8

const (
	PriorityLow    Priority = -1
	PriorityNormal Priority = 0
	PriorityHigh   Priority = 1
)

// Job is the unit of work delivered to a Handler.
type Job struct {
	// ID is the backend-assigned unique identifier (UUID for durable backends,
	// monotonic for inmem). Always non-empty when delivered to a Handler.
	ID string
	// Topic is the fully formatted topic name as it appears in the backend
	// (i.e. after Format).
	Topic string
	// Payload is the opaque body supplied to Enqueue.
	Payload []byte
	// Attempt is 1 on first delivery, 2 on the first retry, and so on.
	Attempt int
	// EnqueuedAt is the time the job was first enqueued (preserved across
	// retries).
	EnqueuedAt time.Time
}

// Handler processes a single job. Returning nil acknowledges the job and the
// backend removes it from the queue. Returning a non-nil error schedules a
// retry according to the configured backoff and max retry count, unless the
// error matches ErrSkipRetry, in which case the job is routed straight to the
// dead letter topic on this attempt.
type Handler func(ctx context.Context, job *Job) error

// Enqueuer enqueues jobs onto a topic.
type Enqueuer interface {
	Enqueue(ctx context.Context, topic string, payload []byte, opts ...EnqueueOption) error
}

// Subscriber registers a Handler against a topic. The returned Consumer
// controls the lifetime of the worker(s).
type Subscriber interface {
	Subscribe(ctx context.Context, topic string, h Handler, opts ...SubscribeOption) (Consumer, error)
}

// Consumer is the lifetime handle for a subscription. Close stops claiming new
// jobs and waits for in-flight handlers up to the configured shutdown timeout.
type Consumer interface {
	Close() error
}

// Service is the union of Enqueuer and Subscriber plus a backend Close.
// Backend implementations expose this as their public type.
type Service interface {
	Enqueuer
	Subscriber
	// Close releases backend resources. Safe to call once; subsequent calls
	// return nil.
	Close(ctx context.Context) error
}

// ErrSkipRetry when returned (or wrapped) from a Handler, signals that the
// job is permanently failed and should be routed to the dead letter topic
// immediately, regardless of remaining retry budget.
var ErrSkipRetry = errors.New("queue: skip retry")

// SkipRetry wraps err so it satisfies errors.Is(err, ErrSkipRetry) while still
// preserving the wrapped error chain. Returning queue.SkipRetry(myErr) from a
// handler routes the job to the dead letter topic on this attempt.
func SkipRetry(err error) error {
	if err == nil {
		return ErrSkipRetry
	}
	return fmt.Errorf("%w: %w", ErrSkipRetry, err)
}
