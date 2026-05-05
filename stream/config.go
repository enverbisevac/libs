package stream

import "time"

const (
	// DefaultAppName matches queue.DefaultAppName / pubsub.DefaultAppName so
	// apps can share namespacing across messaging packages.
	DefaultAppName = "app"
	// DefaultNamespace matches queue.DefaultNamespace / pubsub.DefaultNamespace.
	DefaultNamespace = "default"

	// DefaultMaxRetries matches queue.DefaultMaxRetries. With this value a
	// message gets up to 4 total delivery attempts (Attempt 1..4) before
	// being routed to the DLQ on what would be Attempt 5.
	DefaultMaxRetries = 3

	// DefaultPollInterval is the fallback wake-up cadence for backends that
	// poll. Push-based wake-ups (sync.Cond, LISTEN/NOTIFY, XREADGROUP BLOCK)
	// preempt this where supported.
	DefaultPollInterval = time.Second

	// DefaultVisibilityTimeout is how long a claimed message stays hidden
	// from other workers in the same group before becoming re-claimable.
	DefaultVisibilityTimeout = 30 * time.Second

	// DefaultShutdownTimeout is how long Consumer.Close waits for in-flight
	// handlers before returning.
	DefaultShutdownTimeout = 30 * time.Second

	// DefaultJanitorInterval is the trim/reclaim janitor cadence used by
	// durable backends (sqlite, pgx, redis). The inmem backend reclaims
	// inline on each worker tick.
	DefaultJanitorInterval = 30 * time.Second
)

const delimiter = "."

// FormatStream formats a stream name as "<app>.<ns>.<name>". This matches
// queue.FormatTopic and pubsub.FormatTopic so an application can share
// namespace configuration across all three packages.
func FormatStream(app, ns, name string) string {
	return app + delimiter + ns + delimiter + name
}

const deadLetterSuffix = ".dead"

// DeadLetterStream returns the default DLQ stream name for a given
// already-formatted stream name. A DLQ stream is just another stream;
// subscribe to it to inspect, alert on, or reprocess failed messages.
func DeadLetterStream(formatted string) string {
	return formatted + deadLetterSuffix
}

// StartPosition controls where a brand-new consumer group starts reading
// from. Has no effect when the group already exists; the durable cursor
// wins. The zero value is StartLatest.
type StartPosition int8

const (
	// StartLatest sets the new group's cursor to the highest message ID at
	// the time of group creation. Group sees only messages appended after
	// this point. Default; matches Redis Streams' "$" and Kafka's
	// auto.offset.reset=latest.
	StartLatest StartPosition = iota
	// StartEarliest sets the new group's cursor to "before any message" so
	// the group replays the entire current backlog. Messages already
	// trimmed by MaxLen/MaxAge are gone — there is no recovering them.
	StartEarliest
)
