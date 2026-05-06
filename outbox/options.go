package outbox

import "time"

// Config holds the configuration for the Relay.
type Config struct {
	PollInterval time.Duration
	BatchSize    int
	// Retention is the age at which processed messages become eligible for
	// deletion. Zero (the default) disables cleanup.
	Retention time.Duration
	// CleanupInterval is how often the relay sweeps for expired messages
	// when Retention > 0. Defaults to one hour.
	CleanupInterval time.Duration
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

// WithRetention enables periodic deletion of processed messages older than d.
// A non-positive value disables cleanup.
func WithRetention(d time.Duration) Option {
	return OptionFunc(func(c *Config) {
		c.Retention = d
	})
}

// WithCleanupInterval sets how often the relay deletes expired processed
// messages when retention is enabled. A non-positive value is ignored.
func WithCleanupInterval(d time.Duration) Option {
	return OptionFunc(func(c *Config) {
		if d > 0 {
			c.CleanupInterval = d
		}
	})
}
