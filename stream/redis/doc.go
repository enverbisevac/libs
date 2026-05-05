// Package redis provides a distributed stream.Service backed by Redis
// Streams.
//
// The caller owns the *redis.Client (or redis.UniversalClient); this
// package never opens or closes the connection.
//
// Maps each stream concept to native Redis Streams commands:
//   - Publish      → XADD stream MAXLEN ~ N * payload ... h:... ...
//   - Subscribe    → XGROUP CREATE on first call, then per-worker
//     XREADGROUP loops with BLOCK for low-latency wake-up
//   - Reclaim      → XAUTOCLAIM with min-idle = VisibilityTimeout
//   - Ack          → XACK
//   - DLQ routing  → XADD to <stream>.dead followed by XACK on the source
//
// MaxLen trim is reasserted on every publish via MAXLEN ~. MaxAge trim
// runs as part of the publish path via XTRIM MINID.
package redis
