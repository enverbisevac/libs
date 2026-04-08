package queue

import "time"

const (
	// DefaultAppName matches pubsub.DefaultAppName so apps can share namespacing.
	DefaultAppName = "app"
	// DefaultNamespace matches pubsub.DefaultNamespace.
	DefaultNamespace = "default"

	// DefaultMaxRetries is intentionally conservative compared to libraries
	// like riverqueue (25). Most workloads should override this per-Subscribe.
	DefaultMaxRetries = 3

	// DefaultPollInterval is the fallback wake-up cadence for backends that
	// poll (sqlite, redis idle, pgx fallback). Push-based wake-ups (NOTIFY,
	// BLPOP, Cond) preempt this interval where supported.
	DefaultPollInterval = time.Second

	// DefaultVisibilityTimeout is how long a claimed job stays hidden from
	// other workers before becoming re-claimable.
	DefaultVisibilityTimeout = 30 * time.Second

	// DefaultShutdownTimeout is how long Consumer.Close waits for in-flight
	// handlers before returning.
	DefaultShutdownTimeout = 30 * time.Second
)

const topicDelimiter = "."

// FormatTopic formats a topic name as "<app>.<namespace>.<topic>". This
// matches the pubsub package convention so an application can share namespace
// configuration between pubsub and queue.
func FormatTopic(app, namespace, topic string) string {
	return app + topicDelimiter + namespace + topicDelimiter + topic
}

const deadLetterSuffix = ".dead"

// DeadLetterTopic returns the default dead letter topic name for a given
// already-formatted topic. A dead letter topic is just another queue topic;
// subscribe to it to inspect, alert on, or reprocess failed jobs.
func DeadLetterTopic(formattedTopic string) string {
	return formattedTopic + deadLetterSuffix
}
