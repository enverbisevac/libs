// Package stream provides a Kafka/Redis-Streams-style durable event stream
// with consumer groups (shared work), at-least-once delivery, and replay.
//
// # Reading model
//
// Every Subscribe takes a (stream, group) pair. Members of the same group
// share work — each message is delivered to exactly one member. Different
// groups each get their own copy of every message. To "fan out" to many
// independent consumers, use a unique group name per consumer.
//
// # Delivery and idempotency
//
// Delivery is at-least-once. A message MAY be delivered more than once when:
//
//   - A handler runs longer than the configured visibility timeout. Another
//     worker in the same group reclaims the message and runs the handler
//     again concurrently. Both invocations may succeed.
//   - A handler succeeds but the backend acknowledgement fails. The message
//     is reclaimed after the visibility timeout expires.
//   - A worker crashes mid-handler. The message becomes claimable again
//     after the visibility timeout.
//
// Make handler effects idempotent (use unique keys, upserts, or
// check-then-act inside a transaction). Inspect Message.Attempt to detect
// retries.
//
// # Retries and dead-lettering
//
// Each handler error or visibility-timeout reclaim increments the message's
// Attempt counter. Once Attempt would exceed MaxRetries+1 on the next
// reclaim, or the handler returns an error matching ErrSkipRetry, the
// message is moved to the DLQ stream — by default "<stream>.dead", a normal
// stream you can subscribe to with the same primitive.
//
// # Trim policy
//
// Streams retain everything by default. Pass WithMaxLen / WithMaxAge on
// Publish (or WithDefaultMaxLen / WithDefaultMaxAge on the service) to cap
// growth. Trim policy is reasserted on every publish (last-write-wins),
// matching Redis Streams' XADD MAXLEN semantics.
//
// # Starting position
//
// WithStartFrom and WithStartFromID apply only when the consumer group is
// first created. After that, the group's durable cursor wins; both options
// are ignored on subsequent Subscribe calls to the same group. Default is
// StartLatest — only see messages appended after the group is created.
//
// # Shutdown semantics
//
// Consumer.Close stops claiming new messages and waits up to
// WithShutdownTimeout for in-flight handlers. Handlers exceeding the
// shutdown timeout continue as orphan goroutines until they return on
// their own; Go provides no mechanism to interrupt a running goroutine.
// Their messages will be reclaimed after the visibility timeout.
package stream
