// Package sql provides shared building blocks for database/sql-based
// outbox.Store implementations. Concrete dialect implementations live in
// subpackages such as outbox/sql/postgres and outbox/sql/sqlite.
package sql

import "time"

// Config holds the configuration shared by database/sql outbox stores.
type Config struct {
	TableName  string
	MaxRetries int
	RetryAfter time.Duration
}

// DefaultConfig returns the default configuration used by dialect stores.
func DefaultConfig() Config {
	return Config{
		TableName:  "outbox_messages",
		MaxRetries: 5,
		RetryAfter: 30 * time.Second,
	}
}

// An Option configures a Store instance.
type Option interface {
	Apply(*Config)
}

// OptionFunc is a function that configures a Store config.
type OptionFunc func(*Config)

// Apply calls f(config).
func (f OptionFunc) Apply(config *Config) {
	f(config)
}

// WithTableName sets the outbox table name.
func WithTableName(s string) Option {
	return OptionFunc(func(c *Config) {
		if s != "" {
			c.TableName = s
		}
	})
}

// WithMaxRetries sets the maximum number of retries before a message is marked as failed.
func WithMaxRetries(n int) Option {
	return OptionFunc(func(c *Config) {
		c.MaxRetries = n
	})
}

// WithRetryAfter sets the duration after which a failed message is retried.
func WithRetryAfter(d time.Duration) Option {
	return OptionFunc(func(c *Config) {
		c.RetryAfter = d
	})
}