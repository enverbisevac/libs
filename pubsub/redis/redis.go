package redis

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/enverbisevac/libs/pubsub"
	"github.com/go-logr/logr"
	"github.com/redis/go-redis/v9"
)

type RedisPubSub any

type PubSub struct {
	config   Config
	client   redis.UniversalClient
	mutex    sync.RWMutex
	registry []pubsub.Consumer
}

// NewRedis create an instance of redis PubSub implementation.
func New(client redis.UniversalClient, options ...Option) *PubSub {
	config := Config{
		App:            "app",
		Namespace:      "default",
		HealthInterval: 3 * time.Second,
		SendTimeout:    60,
		ChannelSize:    100,
	}

	for _, f := range options {
		f.Apply(&config)
	}
	return &PubSub{
		config:   config,
		client:   client,
		registry: make([]pubsub.Consumer, 0, 16),
	}
}

// subscribe consumer to process the event with payload.
func (ps *PubSub) subscribe(
	ctx context.Context,
	topic string,
	options ...pubsub.SubscribeOption,
) *redisSubscriber {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	config := pubsub.SubscribeConfig{
		Topics:         make([]string, 0, 8),
		App:            ps.config.App,
		Namespace:      ps.config.Namespace,
		HealthInterval: ps.config.HealthInterval,
		SendTimeout:    ps.config.SendTimeout,
		ChannelSize:    ps.config.ChannelSize,
	}

	for _, f := range options {
		f.Apply(&config)
	}

	// create subscriber and map it to the registry
	subscriber := &redisSubscriber{
		config: &config,
	}

	config.Topics = append(config.Topics, topic)

	topics := subscriber.formatTopics(config.Topics...)
	subscriber.rdb = ps.client.Subscribe(ctx, topics...)

	// start subscriber
	go subscriber.start(ctx)

	// register subscriber
	ps.registry = append(ps.registry, subscriber)

	return subscriber
}

// Subscribe consumer to process the event with payload.
func (ps *PubSub) Subscribe(
	ctx context.Context,
	topic string,
	handler func(msg *pubsub.Msg) error,
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
	output := make(chan *pubsub.Msg)

	subscriber := ps.subscribe(ctx, topic, options...)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ch := <-subscriber.rdb.Channel():
				output <- &pubsub.Msg{
					Topic:   ch.Channel,
					Payload: []byte(ch.Payload),
				}
			}
		}
	}()

	return subscriber, output
}

// Publish event topic to message broker with payload.
func (ps *PubSub) Publish(ctx context.Context, topic string, payload []byte, opts ...pubsub.PublishOption) error {
	pubConfig := pubsub.PublishConfig{
		App:       ps.config.App,
		Namespace: ps.config.Namespace,
	}
	for _, f := range opts {
		f.Apply(&pubConfig)
	}

	topic = pubsub.FormatTopic(pubConfig.App, pubConfig.Namespace, topic)

	err := ps.client.Publish(ctx, topic, payload).Err()
	if err != nil {
		return fmt.Errorf("failed to write to pubsub topic '%s'. Error: %w",
			topic, err)
	}
	return nil
}

func (r *PubSub) Close(_ context.Context) error {
	for _, subscriber := range r.registry {
		err := subscriber.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

type redisSubscriber struct {
	config  *pubsub.SubscribeConfig
	rdb     *redis.PubSub
	handler func(msg *pubsub.Msg) error
}

func (s *redisSubscriber) start(ctx context.Context) {
	log := logr.FromContextOrDiscard(ctx)
	// Go channel which receives messages.
	ch := s.rdb.Channel(
		redis.WithChannelHealthCheckInterval(s.config.HealthInterval),
		redis.WithChannelSendTimeout(s.config.SendTimeout),
		redis.WithChannelSize(s.config.ChannelSize),
	)
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				log.Info("redis channel was closed")
				return
			}
			if err := s.handler(&pubsub.Msg{
				Topic:   msg.Channel,
				Payload: []byte(msg.Payload),
			}); err != nil {
				log.Error(err, "received an error from handler function")
			}
		}
	}
}

func (s *redisSubscriber) Subscribe(ctx context.Context, topics ...string) error {
	err := s.rdb.Subscribe(ctx, s.formatTopics(topics...)...)
	if err != nil {
		return fmt.Errorf("subscribe failed for chanels %v with error: %w",
			strings.Join(topics, ","), err)
	}
	return nil
}

func (s *redisSubscriber) Unsubscribe(ctx context.Context, topics ...string) error {
	err := s.rdb.Unsubscribe(ctx, s.formatTopics(topics...)...)
	if err != nil {
		return fmt.Errorf("unsubscribe failed for chanels %v with error: %w",
			strings.Join(topics, ","), err)
	}
	return nil
}

func (s *redisSubscriber) Close() error {
	err := s.rdb.Close()
	if err != nil {
		return fmt.Errorf("failed while closing subscriber with error: %w", err)
	}
	return nil
}

func (s *redisSubscriber) formatTopics(topics ...string) []string {
	result := make([]string, len(topics))
	for i, topic := range topics {
		result[i] = pubsub.FormatTopic(s.config.App, s.config.Namespace, topic)
	}
	return result
}
