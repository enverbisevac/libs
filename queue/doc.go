// Package queue provides a work queue with single-worker delivery semantics
// (competing consumers). Each enqueued job is processed by exactly one worker
// at a time. Delivery is at-least-once: handlers MUST be idempotent.
//
// # Idempotency contract
//
// A job MAY be delivered to a handler more than once when:
//
//   - A handler runs longer than the configured visibility timeout. After the
//     visibility timeout expires, another worker may re-claim the same job and
//     run the handler again concurrently. Both invocations may succeed.
//   - A handler succeeds but the backend acknowledgement (DELETE / ack) fails.
//     The job will be re-delivered after the visibility timeout.
//   - A worker crashes mid-handler. The job becomes visible again after the
//     visibility timeout and will be re-delivered.
//
// Make handler effects idempotent (use unique keys, upserts, or check-then-act
// patterns inside a transaction). Do not assume "the job ran" implies "exactly
// one effect was produced".
//
// # Shutdown behavior
//
// Consumer.Close stops claiming new jobs and waits up to WithShutdownTimeout
// for in-flight handlers to return. Handlers that exceed the shutdown timeout
// continue running as orphaned goroutines until they return on their own; Go
// provides no mechanism to interrupt a running goroutine. Their jobs will be
// re-claimed after the visibility timeout if they outlive the worker process.
//
// # Topics with no subscribers
//
// Durable backends (sqlite, pgx, redis) accumulate jobs for topics that have
// no subscriber. There is no automatic expiry. Operators are responsible for
// monitoring queue depth.
package queue
