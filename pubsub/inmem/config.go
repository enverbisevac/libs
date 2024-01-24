package inmem

import "time"

type Config struct {
	App       string
	Namespace string

	SendTimeout time.Duration
	ChannelSize int
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
		m.App = value
	})
}

// WithNamespace returns an option that set config namespace.
func WithNamespace(value string) Option {
	return OptionFunc(func(m *Config) {
		m.Namespace = value
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
