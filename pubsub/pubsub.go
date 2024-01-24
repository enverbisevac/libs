package pubsub

import "context"

type Msg struct {
	Topic   string
	Payload []byte
}

type Publisher interface {
	// Publish topic to message broker with payload.
	Publish(ctx context.Context, topic string, payload []byte,
		options ...PublishOption) error
}

type Consumer interface {
	Subscribe(ctx context.Context, topics ...string) error
	Unsubscribe(ctx context.Context, topics ...string) error
	Close() error
}

type Subscriber interface {
	Subscribe(ctx context.Context, topic string,
		handler func(payload *Msg) error, options ...SubscribeOption) Consumer
	SubscribeChan(ctx context.Context, topic string,
		options ...SubscribeOption) (Consumer, <-chan *Msg)
}
