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

type RedisPubSub interface {
}

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

// Subscribe consumer to process the event with payload.
func (r *PubSub) Subscribe(
	ctx context.Context,
	topic string,
	handler func(payload []byte) error,
	options ...pubsub.SubscribeOption,
) pubsub.Consumer {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	config := pubsub.SubscribeConfig{
		Topics:         make([]string, 0, 8),
		App:            r.config.App,
		Namespace:      r.config.Namespace,
		HealthInterval: r.config.HealthInterval,
		SendTimeout:    r.config.SendTimeout,
		ChannelSize:    r.config.ChannelSize,
	}

	for _, f := range options {
		f.Apply(&config)
	}

	// create subscriber and map it to the registry
	subscriber := &redisSubscriber{
		config:  &config,
		handler: handler,
	}

	config.Topics = append(config.Topics, topic)

	topics := subscriber.formatTopics(config.Topics...)
	subscriber.rdb = r.client.Subscribe(ctx, topics...)

	// start subscriber
	go subscriber.start(ctx)

	// register subscriber
	r.registry = append(r.registry, subscriber)

	return subscriber
}

// Publish event topic to message broker with payload.
func (r *PubSub) Publish(ctx context.Context, topic string, payload []byte, opts ...pubsub.PublishOption) error {
	pubConfig := pubsub.PublishConfig{
		App:       r.config.App,
		Namespace: r.config.Namespace,
	}
	for _, f := range opts {
		f.Apply(&pubConfig)
	}

	topic = pubsub.FormatTopic(pubConfig.App, pubConfig.Namespace, topic)

	err := r.client.Publish(ctx, topic, payload).Err()
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
	handler func([]byte) error
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
			if err := s.handler([]byte(msg.Payload)); err != nil {
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
