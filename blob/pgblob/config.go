package pgblob

// Config holds the configuration for the PostgreSQL blob bucket.
type Config struct {
	TableName string
}

// An Option configures a bucket instance.
type Option interface {
	Apply(*Config)
}

// OptionFunc is a function that configures a bucket config.
type OptionFunc func(*Config)

// Apply calls f(config).
func (f OptionFunc) Apply(config *Config) {
	f(config)
}

// WithTableName sets the blob table name.
func WithTableName(s string) Option {
	return OptionFunc(func(c *Config) {
		if s != "" {
			c.TableName = s
		}
	})
}
