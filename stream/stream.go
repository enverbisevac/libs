package stream

import (
	"context"
	"time"
)

// Message is a single entry delivered to a Handler.
type Message struct {
	// ID is the backend-assigned identifier, monotonic per stream. Always
	// non-empty when delivered to a Handler.
	ID string
	// Stream is the fully formatted stream name as it appears in the backend
	// (after FormatStream).
	Stream string
	// Payload is the opaque body supplied to Publish.
	Payload []byte
	// Headers are the caller-supplied key-value metadata. May be nil if no
	// headers were supplied.
	Headers map[string]string
	// CreatedAt is the time the message was first appended to the stream.
	CreatedAt time.Time
	// Attempt is 1 on first delivery, 2+ on every redelivery (visibility
	// timeout reclaim or handler-error retry).
	Attempt int
}

// Handler processes a single message. Returning nil acks the message and
// the backend removes it from the consumer group's pending list. Returning
// a non-nil error leaves the message pending; it becomes available for
// re-claim by another worker after the configured visibility timeout. If
// Attempt would exceed MaxRetries+1 on the next reclaim, or the error
// matches ErrSkipRetry, the message is routed to the DLQ stream instead.
type Handler func(ctx context.Context, msg *Message) error

// Producer publishes messages to a stream.
type Producer interface {
	Publish(ctx context.Context, stream string, payload []byte,
		opts ...PublishOption) error
}

// Subscriber registers a Handler against a (stream, consumer-group) pair.
// Members of the same group share work — each message is delivered to
// exactly one member.
type Subscriber interface {
	Subscribe(ctx context.Context, stream, group string, h Handler,
		opts ...SubscribeOption) (Consumer, error)
}

// Consumer is the lifetime handle for a subscription. Close stops claiming
// new messages and waits for in-flight handlers up to the configured
// shutdown timeout.
type Consumer interface {
	Close() error
}

// Service is the union of Producer and Subscriber plus a backend Close.
// Backend implementations expose this as their public type.
type Service interface {
	Producer
	Subscriber
	// Close releases backend resources and shuts down all live consumers.
	// Safe to call once; subsequent calls return nil.
	Close(ctx context.Context) error
}
