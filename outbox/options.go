package outbox

import "time"

// Config holds the configuration for the Relay.
type Config struct {
	PollInterval time.Duration
	BatchSize    int
}

// An Option configures a Relay instance.
type Option interface {
	Apply(*Config)
}

// OptionFunc is a function that configures a Relay config.
type OptionFunc func(*Config)

// Apply calls f(config).
func (f OptionFunc) Apply(config *Config) {
	f(config)
}

// WithPollInterval sets how often the relay polls for pending messages.
func WithPollInterval(d time.Duration) Option {
	return OptionFunc(func(c *Config) {
		c.PollInterval = d
	})
}

// WithBatchSize sets the maximum number of messages fetched per poll cycle.
func WithBatchSize(n int) Option {
	return OptionFunc(func(c *Config) {
		c.BatchSize = n
	})
}
