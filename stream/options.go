package stream

import "time"

// ServiceConfig is the service-level configuration shared by all Publish and
// Subscribe calls on a Service.
type ServiceConfig struct {
	App           string
	Namespace     string
	DefaultMaxLen int64
	DefaultMaxAge time.Duration
}

// ServiceOption configures a backend constructor.
type ServiceOption interface {
	Apply(*ServiceConfig)
}

// ServiceOptionFunc adapts a function to ServiceOption.
type ServiceOptionFunc func(*ServiceConfig)

// Apply calls f(cfg).
func (f ServiceOptionFunc) Apply(cfg *ServiceConfig) { f(cfg) }

// WithApp sets the service-level app name. Empty string is ignored so
// callers can pass values from optional config without clobbering presets.
func WithApp(app string) ServiceOption {
	return ServiceOptionFunc(func(c *ServiceConfig) {
		if app != "" {
			c.App = app
		}
	})
}

// WithNamespace sets the service-level namespace. Empty string is ignored.
func WithNamespace(ns string) ServiceOption {
	return ServiceOptionFunc(func(c *ServiceConfig) {
		if ns != "" {
			c.Namespace = ns
		}
	})
}

// WithDefaultMaxLen sets the default per-publish MaxLen used when a
// Publish call doesn't provide one. Zero means no trimming by length.
func WithDefaultMaxLen(n int64) ServiceOption {
	return ServiceOptionFunc(func(c *ServiceConfig) { c.DefaultMaxLen = n })
}

// WithDefaultMaxAge sets the default per-publish MaxAge used when a
// Publish call doesn't provide one. Zero means no trimming by age.
func WithDefaultMaxAge(d time.Duration) ServiceOption {
	return ServiceOptionFunc(func(c *ServiceConfig) { c.DefaultMaxAge = d })
}

// ResolveServiceConfig fills package defaults into any zero-valued fields
// on cfg. Backend constructors should call this once after applying
// user-supplied options.
func ResolveServiceConfig(cfg *ServiceConfig) {
	if cfg.App == "" {
		cfg.App = DefaultAppName
	}
	if cfg.Namespace == "" {
		cfg.Namespace = DefaultNamespace
	}
}

// PublishConfig is the resolved configuration for a single Publish call.
// Backends populate App, Namespace, MaxLen, MaxAge defaults from their
// service-level config, then apply PublishOptions to overlay caller intent.
type PublishConfig struct {
	App       string
	Namespace string
	Headers   map[string]string
	MaxLen    int64
	MaxAge    time.Duration
}

// PublishOption configures a single Publish call.
type PublishOption interface {
	Apply(*PublishConfig)
}

// PublishOptionFunc adapts a function to PublishOption.
type PublishOptionFunc func(*PublishConfig)

// Apply calls f(cfg).
func (f PublishOptionFunc) Apply(cfg *PublishConfig) { f(cfg) }

// WithHeaders attaches a metadata map to the message. The map is shallow-
// copied by the backend; nil is allowed and equivalent to "no headers".
func WithHeaders(h map[string]string) PublishOption {
	return PublishOptionFunc(func(c *PublishConfig) { c.Headers = h })
}

// WithMaxLen overrides the service-level DefaultMaxLen for this publish.
// Zero means "no trim" for this call. Trim policy is reasserted by every
// publisher (last-write-wins per Redis Streams XADD MAXLEN semantics).
func WithMaxLen(n int64) PublishOption {
	return PublishOptionFunc(func(c *PublishConfig) { c.MaxLen = n })
}

// WithMaxAge overrides the service-level DefaultMaxAge for this publish.
// Zero means "no trim" for this call.
func WithMaxAge(d time.Duration) PublishOption {
	return PublishOptionFunc(func(c *PublishConfig) { c.MaxAge = d })
}

// WithPublishApp overrides the topic app prefix for this publish only.
func WithPublishApp(app string) PublishOption {
	return PublishOptionFunc(func(c *PublishConfig) {
		if app != "" {
			c.App = app
		}
	})
}

// WithPublishNamespace overrides the topic namespace for this publish only.
func WithPublishNamespace(ns string) PublishOption {
	return PublishOptionFunc(func(c *PublishConfig) {
		if ns != "" {
			c.Namespace = ns
		}
	})
}

// SubscribeConfig is the resolved configuration for a single Subscribe call.
type SubscribeConfig struct {
	App       string
	Namespace string

	StartFrom   StartPosition
	StartFromID string // overrides StartFrom when non-empty

	Concurrency       int
	MaxRetries        int
	PollInterval      time.Duration
	VisibilityTimeout time.Duration
	ShutdownTimeout   time.Duration

	// DeadLetterStream is the stream that exhausted messages are routed
	// to. Zero value means "<formatted-stream>.dead".
	DeadLetterStream string
}

// SubscribeOption configures a single Subscribe call.
type SubscribeOption interface {
	Apply(*SubscribeConfig)
}

// SubscribeOptionFunc adapts a function to SubscribeOption.
type SubscribeOptionFunc func(*SubscribeConfig)

// Apply calls f(cfg).
func (f SubscribeOptionFunc) Apply(cfg *SubscribeConfig) { f(cfg) }

// WithStartFrom controls where a brand-new consumer group starts reading.
// Has no effect when the group already exists.
func WithStartFrom(p StartPosition) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) { c.StartFrom = p })
}

// WithStartFromID sets a brand-new group's cursor to a specific message ID.
// Group sees messages with ID strictly greater than id. Overrides
// WithStartFrom. Has no effect when the group already exists.
func WithStartFromID(id string) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) { c.StartFromID = id })
}

// WithConcurrency sets the number of worker goroutines for this consumer.
// Default 1. Values < 1 clamp to 1.
func WithConcurrency(n int) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		if n < 1 {
			n = 1
		}
		c.Concurrency = n
	})
}

// WithMaxRetries sets how many retries are allowed before routing to the
// DLQ. Default 3 (= 4 total attempts). -1 means infinite retries.
func WithMaxRetries(n int) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) { c.MaxRetries = n })
}

// WithDeadLetterStream sets a custom DLQ stream. Default
// "<formatted-stream>.dead".
func WithDeadLetterStream(name string) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) { c.DeadLetterStream = name })
}

// WithPollInterval sets the fallback wake-up cadence for backends that
// poll. Push wake-ups (sync.Cond, NOTIFY, XREADGROUP BLOCK) preempt this.
func WithPollInterval(d time.Duration) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) { c.PollInterval = d })
}

// WithVisibilityTimeout sets how long a claimed message stays hidden from
// other workers in the same group. Also acts as the implicit retry delay
// when a handler returns an error. Handlers exceeding this timeout will
// see their message reclaimed by another worker.
func WithVisibilityTimeout(d time.Duration) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) { c.VisibilityTimeout = d })
}

// WithShutdownTimeout sets how long Consumer.Close waits for in-flight
// handlers before returning.
func WithShutdownTimeout(d time.Duration) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) { c.ShutdownTimeout = d })
}

// WithSubscribeApp overrides the topic app prefix for this subscription.
func WithSubscribeApp(app string) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		if app != "" {
			c.App = app
		}
	})
}

// WithSubscribeNamespace overrides the topic namespace for this subscription.
func WithSubscribeNamespace(ns string) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		if ns != "" {
			c.Namespace = ns
		}
	})
}

// ResolveSubscribeConfig fills package defaults for any zero-valued field
// in cfg. Backend implementations call this once per Subscribe after
// applying user options. formattedStream is the result of FormatStream
// applied to the caller-supplied stream name.
func ResolveSubscribeConfig(cfg *SubscribeConfig, formattedStream string) {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = DefaultVisibilityTimeout
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}
	if cfg.DeadLetterStream == "" {
		cfg.DeadLetterStream = DeadLetterStream(formattedStream)
	}
}
