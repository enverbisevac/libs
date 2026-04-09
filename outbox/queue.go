package outbox

import (
	"context"

	"github.com/enverbisevac/libs/queue"
)

var _ Publisher = (*QueueAdapter)(nil)

// QueueAdapter wraps a queue.Enqueuer to satisfy the outbox.Publisher
// interface. Use this when you want the outbox relay to deliver messages as
// work items processed by exactly one consumer (competing consumers), instead
// of broadcasting them to all subscribers via pubsub.
type QueueAdapter struct {
	enq  queue.Enqueuer
	opts []queue.EnqueueOption
}

// NewQueueAdapter creates a new QueueAdapter. opts are applied to every
// Enqueue call.
func NewQueueAdapter(e queue.Enqueuer, opts ...queue.EnqueueOption) *QueueAdapter {
	return &QueueAdapter{
		enq:  e,
		opts: opts,
	}
}

// Publish forwards outbox messages to the underlying queue.Enqueuer.
func (a *QueueAdapter) Publish(ctx context.Context, msgs ...Message) error {
	for _, msg := range msgs {
		if err := a.enq.Enqueue(ctx, msg.Topic, msg.Payload, a.opts...); err != nil {
			return err
		}
	}
	return nil
}
