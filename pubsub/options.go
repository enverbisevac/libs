package pubsub

import "time"

type PublishConfig struct {
	App       string
	Namespace string
}

func (c *PublishConfig) Apply(pc *PublishConfig) {
	if c == nil {
		return
	}
	*c = *pc
}

type PublishOption interface {
	Apply(*PublishConfig)
}

// PublishOptionFunc is a function that configures a publish config.
type PublishOptionFunc func(*PublishConfig)

// Apply calls f(publishConfig).
func (f PublishOptionFunc) Apply(config *PublishConfig) {
	f(config)
}

// WithPublishApp modifies publish config app identifier.
func WithPublishApp(value string) PublishOption {
	return PublishOptionFunc(func(c *PublishConfig) {
		c.App = value
	})
}

// WithPublishNamespace modifies publish config namespace.
func WithPublishNamespace(value string) PublishOption {
	return PublishOptionFunc(func(c *PublishConfig) {
		c.Namespace = value
	})
}

type SubscribeConfig struct {
	Topics         []string
	App            string
	Namespace      string
	HealthInterval time.Duration
	SendTimeout    time.Duration
	ChannelSize    int
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
		c.Topics = topics
	})
}

// WithNamespace returns an channel option that configures namespace.
func WithChannelNamespace(value string) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		c.Namespace = value
	})
}

// WithChannelHealthCheckInterval specifies the channel health check interval.
// PubSub will ping Server if it does not receive any messages
// within the interval. To disable health check, use zero interval.
func WithChannelHealthCheckInterval(value time.Duration) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		c.HealthInterval = value
	})
}

// WithChannelSendTimeout specifies the channel send timeout after which
// the message is dropped.
func WithChannelSendTimeout(value time.Duration) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		c.SendTimeout = value
	})
}

// WithChannelSize specifies the Go chan size that is used to buffer
// incoming messages for subscriber.
func WithChannelSize(value int) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		c.ChannelSize = value
	})
}

func FormatTopic(app, ns, topic string) string {
	return app + ":" + ns + ":" + topic
}
