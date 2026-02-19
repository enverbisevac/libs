package outbox

import (
	"context"

	"github.com/enverbisevac/libs/pubsub"
)

var _ Publisher = (*PubSubAdapter)(nil)

// PubSubAdapter wraps a pubsub.Publisher to satisfy the outbox.Publisher interface.
type PubSubAdapter struct {
	pub  pubsub.Publisher
	opts []pubsub.PublishOption
}

// NewPubSubAdapter creates a new PubSubAdapter.
func NewPubSubAdapter(p pubsub.Publisher, opts ...pubsub.PublishOption) *PubSubAdapter {
	return &PubSubAdapter{
		pub:  p,
		opts: opts,
	}
}

// Publish forwards outbox messages to the underlying pubsub.Publisher.
func (a *PubSubAdapter) Publish(ctx context.Context, msgs ...Message) error {
	for _, msg := range msgs {
		if err := a.pub.Publish(ctx, msg.Topic, msg.Payload, a.opts...); err != nil {
			return err
		}
	}
	return nil
}
