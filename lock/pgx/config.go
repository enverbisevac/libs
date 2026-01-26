package pgx

// Config holds the configuration for the pgx lock service.
type Config struct {
	// Namespace is used to separate locks from different applications.
	Namespace int32
}

// Option configures a lock service instance.
type Option interface {
	Apply(*Config)
}

// OptionFunc is a function that configures a lock config.
type OptionFunc func(*Config)

// Apply calls f(config).
func (f OptionFunc) Apply(config *Config) {
	f(config)
}

// WithNamespace returns an option that sets the namespace for locks.
// The namespace is used as the first key in PostgreSQL advisory locks.
func WithNamespace(value int32) Option {
	return OptionFunc(func(c *Config) {
		c.Namespace = value
	})
}