package inmem

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/enverbisevac/libs/pubsub"
	"github.com/go-logr/logr"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

type PubSub struct {
	config   Config
	mutex    sync.RWMutex
	registry []*inMemorySubscriber
}

// New create an instance of memory pubsub implementation.
func New(options ...Option) *PubSub {
	config := Config{
		App:         "app",
		Namespace:   "default",
		SendTimeout: 10 * time.Second,
		ChannelSize: 100,
	}

	for _, f := range options {
		f.Apply(&config)
	}
	return &PubSub{
		config:   config,
		registry: make([]*inMemorySubscriber, 0, 16),
	}
}

// Subscribe consumer to process the event with payload.
func (ps *PubSub) subscribe(
	_ context.Context,
	topic string,
	options ...pubsub.SubscribeOption,
) *inMemorySubscriber {
	config := pubsub.SubscribeConfig{
		Topics:      make([]string, 0, 8),
		App:         ps.config.App,
		Namespace:   ps.config.Namespace,
		SendTimeout: ps.config.SendTimeout,
		ChannelSize: ps.config.ChannelSize,
	}

	for _, f := range options {
		f.Apply(&config)
	}

	// create subscriber and map it to the registry
	subscriber := &inMemorySubscriber{
		config:  &config,
		channel: make(chan *pubsub.Msg, ps.config.ChannelSize),
		onClose: func(subscriber *inMemorySubscriber) {
			ps.mutex.Lock()
			ps.registry = slices.DeleteFunc(ps.registry, func(s *inMemorySubscriber) bool {
				return s == subscriber
			})
			ps.mutex.Unlock()
		},
	}

	config.Topics = append(config.Topics, topic)
	subscriber.topics = subscriber.formatTopics(config.Topics...)

	// register subscriber
	ps.mutex.Lock()
	ps.registry = append(ps.registry, subscriber)
	ps.mutex.Unlock()

	return subscriber
}

// Subscribe consumer to process the event with payload.
func (ps *PubSub) Subscribe(
	ctx context.Context,
	topic string,
	handler func(payload *pubsub.Msg) error,
	options ...pubsub.SubscribeOption,
) pubsub.Consumer {
	subscriber := ps.subscribe(ctx, topic, options...)
	subscriber.handler = handler
	go subscriber.start(ctx)
	return subscriber
}

func (ps *PubSub) SubscribeChan(
	ctx context.Context,
	topic string,
	options ...pubsub.SubscribeOption,
) (pubsub.Consumer, <-chan *pubsub.Msg) {
	subscriber := ps.subscribe(ctx, topic, options...)
	return subscriber, subscriber.channel
}

// Publish event to message broker with payload.
func (ps *PubSub) Publish(ctx context.Context, topic string, payload []byte, opts ...pubsub.PublishOption) error {
	log := logr.FromContextOrDiscard(ctx)

	ctx = context.WithoutCancel(ctx)

	ps.mutex.RLock()
	registry := make([]*inMemorySubscriber, len(ps.registry))
	copy(registry, ps.registry)
	ps.mutex.RUnlock()

	if len(registry) == 0 {
		log.V(1).Info("in pubsub Publish: no subscribers registered")
		return nil
	}

	pubConfig := pubsub.PublishConfig{
		App:       ps.config.App,
		Namespace: ps.config.Namespace,
	}
	for _, f := range opts {
		f.Apply(&pubConfig)
	}

	topic = pubsub.FormatTopic(pubConfig.App, pubConfig.Namespace, topic)
	g, ctx := errgroup.WithContext(ctx)
	for _, sub := range registry {
		if sub.HasTopic(topic) && !sub.isClosed() {
			g.Go(func() error {
				// timer is based on subscriber data
				t := time.NewTimer(sub.config.SendTimeout)
				defer t.Stop()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case sub.channel <- &pubsub.Msg{Topic: topic, Payload: payload}:
					log.V(1).Info(fmt.Sprintf("in pubsub Publish: message %v sent to topic %s", string(payload), topic))
					return nil
				case <-t.C:
					// channel is full for topic (message is dropped)
					return fmt.Errorf("in pubsub Publish: %s topic is full for %s (message is dropped)",
						topic, sub.config.SendTimeout)
				}
			})
		}
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func (r *PubSub) Close(_ context.Context) error {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	for _, subscriber := range r.registry {
		if err := subscriber.Close(); err != nil {
			return err
		}
	}
	return nil
}

type inMemorySubscriber struct {
	config  *pubsub.SubscribeConfig
	handler func(*pubsub.Msg) error
	channel chan *pubsub.Msg
	mutex   sync.RWMutex
	topics  []string
	closed  bool

	onClose func(subscriber *inMemorySubscriber)
}

func (s *inMemorySubscriber) start(ctx context.Context) {
	log := logr.FromContextOrDiscard(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-s.channel:
			if !ok {
				return
			}
			if err := s.handler(msg); err != nil {
				// TODO: bump err to caller
				log.Error(err, "in pubsub start: error while running handler for topic")
			}
		}
	}
}

func (s *inMemorySubscriber) Subscribe(_ context.Context, topics ...string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	topics = s.formatTopics(topics...)
	for _, ch := range topics {
		if slices.Contains(s.topics, ch) {
			continue
		}
		s.topics = append(s.topics, ch)
	}
	return nil
}

func (s *inMemorySubscriber) Unsubscribe(_ context.Context, topics ...string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	topics = s.formatTopics(topics...)
	for i, ch := range topics {
		if slices.Contains(s.topics, ch) {
			s.topics[i] = s.topics[len(s.topics)-1]
			s.topics = s.topics[:len(s.topics)-1]
		}
	}
	return nil
}

func (s *inMemorySubscriber) HasTopic(topic string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return slices.Contains(s.topics, topic)
}

func (s *inMemorySubscriber) Close() error {
	if s.isClosed() {
		return nil
	}

	s.mutex.Lock()
	s.closed = true
	s.mutex.Unlock()

	close(s.channel)

	if s.onClose != nil {
		s.onClose(s)
	}

	return nil
}

func (s *inMemorySubscriber) isClosed() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.closed
}

func (s *inMemorySubscriber) formatTopics(topics ...string) []string {
	result := make([]string, len(topics))
	for i, topic := range topics {
		result[i] = pubsub.FormatTopic(s.config.App, s.config.Namespace, topic)
	}
	return result
}
