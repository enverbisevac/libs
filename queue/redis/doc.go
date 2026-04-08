// Package redis provides a durable queue.Service backed by Redis.
//
// Data layout (per topic) — keys use Redis hash tags so all per-topic state
// hashes to the same slot in Redis Cluster:
//
//	queue:{<topic>}:ready    Sorted Set, score = -priority * 1e13 + run_at_ms
//	queue:{<topic>}:inflight Sorted Set, score = locked_until_ms
//	queue:{<topic>}:job:<id> Hash with fields: payload, priority, attempt,
//	                         enqueued_at, last_error
//	queue:{<topic>}:notify   List used with BLPOP for push wake-up
//
// All single-topic operations (enqueue, claim, ack, fail, reap) are Lua
// scripts so multi-key updates are atomic from Redis's perspective. Dead
// letter routing crosses topics and is therefore split into two Go-side
// scripts (enqueue into DLQ, then ack source); a failure between the two
// produces an at-least-once DLQ delivery, which matches the package's
// overall delivery contract.
//
// A reaper goroutine periodically scans inflight for entries whose lease has
// expired and moves them back to ready.
package redis
