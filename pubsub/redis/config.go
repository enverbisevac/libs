package redis

import "time"

type Config struct {
	App       string // app namespace prefix
	Namespace string

	HealthInterval time.Duration
	SendTimeout    time.Duration
	ChannelSize    int
}

// An Option configures a pubsub instance.
type Option interface {
	Apply(*Config)
}

// OptionFunc is a function that configures a pubsub config.
type OptionFunc func(*Config)

// Apply calls f(config).
func (f OptionFunc) Apply(config *Config) {
	f(config)
}

// WithApp returns an option that set config app name.
func WithApp(value string) Option {
	return OptionFunc(func(m *Config) {
		if value != "" {
			m.App = value
		}
	})
}

// WithNamespace returns an option that set config namespace.
func WithNamespace(value string) Option {
	return OptionFunc(func(m *Config) {
		if value != "" {
			m.Namespace = value
		}
	})
}

// WithHealthCheckInterval specifies the config health check interval.
// PubSub will ping Server if it does not receive any messages
// within the interval (redis, ...).
// To disable health check, use zero interval.
func WithHealthCheckInterval(value time.Duration) Option {
	return OptionFunc(func(m *Config) {
		m.HealthInterval = value
	})
}

// WithSendTimeout specifies the pubsub send timeout after which
// the message is dropped.
func WithSendTimeout(value time.Duration) Option {
	return OptionFunc(func(m *Config) {
		m.SendTimeout = value
	})
}

// WithSize specifies the Go chan size in config that is used to buffer
// incoming messages.
func WithSize(value int) Option {
	return OptionFunc(func(m *Config) {
		m.ChannelSize = value
	})
}

type SubscribeConfig struct {
	topics         []string
	namespace      string
	healthInterval time.Duration
	sendTimeout    time.Duration
	channelSize    int
}

// SubscribeOption configures a subscription config.
type SubscribeOption interface {
	Apply(*SubscribeConfig)
}

// SubscribeOptionFunc is a function that configures a subscription config.
type SubscribeOptionFunc func(*SubscribeConfig)

// Apply calls f(subscribeConfig).
func (f SubscribeOptionFunc) Apply(config *SubscribeConfig) {
	f(config)
}

// WithTopics specifies the topics to subsribe.
func WithTopics(topics ...string) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		c.topics = topics
	})
}

// WithChannelNamespace returns an channel option that configures namespace.
func WithChannelNamespace(value string) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		if value != "" {
			c.namespace = value
		}
	})
}

// WithChannelHealthCheckInterval specifies the channel health check interval.
// PubSub will ping Server if it does not receive any messages
// within the interval. To disable health check, use zero interval.
func WithChannelHealthCheckInterval(value time.Duration) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		c.healthInterval = value
	})
}

// WithChannelSendTimeout specifies the channel send timeout after which
// the message is dropped.
func WithChannelSendTimeout(value time.Duration) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		c.sendTimeout = value
	})
}

// WithChannelSize specifies the Go chan size used to buffer
// incoming messages for subscriber.
func WithChannelSize(value int) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		c.channelSize = value
	})
}
