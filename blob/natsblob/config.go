package natsblob

// Config holds the configuration for the NATS blob bucket.
type Config struct{}

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
