package queue

import "time"

// EnqueueConfig is the resolved configuration for a single Enqueue call.
// Backends populate the App, Namespace, and Priority defaults from their own
// service-level config, then apply EnqueueOptions to overlay caller intent.
type EnqueueConfig struct {
	App       string
	Namespace string
	Priority  Priority
	// RunAt is the absolute time at which the job becomes eligible. Zero
	// means "now". WithDelay and WithRunAt both populate this field; WithRunAt
	// always wins (last-wins, but RunAt is also "stronger" semantically).
	RunAt time.Time
}

// EnqueueOption configures a single Enqueue call.
type EnqueueOption interface {
	Apply(*EnqueueConfig)
}

// EnqueueOptionFunc adapts a function to EnqueueOption.
type EnqueueOptionFunc func(*EnqueueConfig)

// Apply calls f(cfg).
func (f EnqueueOptionFunc) Apply(cfg *EnqueueConfig) { f(cfg) }

// WithDelay schedules the job to run no earlier than now+d.
func WithDelay(d time.Duration) EnqueueOption {
	return EnqueueOptionFunc(func(c *EnqueueConfig) {
		c.RunAt = time.Now().Add(d)
	})
}

// WithRunAt schedules the job to run no earlier than t. Overrides WithDelay.
func WithRunAt(t time.Time) EnqueueOption {
	return EnqueueOptionFunc(func(c *EnqueueConfig) { c.RunAt = t })
}

// WithPriority assigns the job's priority.
func WithPriority(p Priority) EnqueueOption {
	return EnqueueOptionFunc(func(c *EnqueueConfig) { c.Priority = p })
}

// WithEnqueueApp overrides the topic app prefix for this enqueue only.
func WithEnqueueApp(app string) EnqueueOption {
	return EnqueueOptionFunc(func(c *EnqueueConfig) {
		if app != "" {
			c.App = app
		}
	})
}

// WithEnqueueNamespace overrides the topic namespace for this enqueue only.
func WithEnqueueNamespace(ns string) EnqueueOption {
	return EnqueueOptionFunc(func(c *EnqueueConfig) {
		if ns != "" {
			c.Namespace = ns
		}
	})
}

// SubscribeConfig is the resolved configuration for a single Subscribe call.
type SubscribeConfig struct {
	App       string
	Namespace string

	Concurrency       int
	MaxRetries        int
	PollInterval      time.Duration
	VisibilityTimeout time.Duration
	ShutdownTimeout   time.Duration

	// DeadLetterTopic is the topic that exhausted jobs are routed to. Zero
	// value means "<topic>.dead".
	DeadLetterTopic string

	// Backoff returns the delay before the given retry attempt. Zero value
	// means DefaultBackoff.
	Backoff BackoffFunc
}

// SubscribeOption configures a single Subscribe call.
type SubscribeOption interface {
	Apply(*SubscribeConfig)
}

// SubscribeOptionFunc adapts a function to SubscribeOption.
type SubscribeOptionFunc func(*SubscribeConfig)

// Apply calls f(cfg).
func (f SubscribeOptionFunc) Apply(cfg *SubscribeConfig) { f(cfg) }

// WithConcurrency sets the number of worker goroutines for this subscription.
// Default 1. Values < 1 are clamped to 1.
func WithConcurrency(n int) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) {
		if n < 1 {
			n = 1
		}
		c.Concurrency = n
	})
}

// WithMaxRetries sets the maximum number of retries before routing to the
// dead letter topic. -1 means infinite retries. Default DefaultMaxRetries.
func WithMaxRetries(n int) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) { c.MaxRetries = n })
}

// WithBackoff overrides the default backoff function.
func WithBackoff(b BackoffFunc) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) { c.Backoff = b })
}

// WithDeadLetterTopic sets a custom dead letter topic. Default
// "<original-topic>.dead".
func WithDeadLetterTopic(topic string) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) { c.DeadLetterTopic = topic })
}

// WithPollInterval sets how often a worker re-polls the backend when idle.
// Push-based wake-ups (NOTIFY, BLPOP, Cond) preempt this interval where
// supported.
func WithPollInterval(d time.Duration) SubscribeOption {
	return SubscribeOptionFunc(func(c *SubscribeConfig) { c.PollInterval = d })
}

// WithVisibilityTimeout sets how long a claimed job stays hidden from other
// workers. Handlers exceeding this timeout will see their job re-claimed.
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

// QueueConfig is the service-level configuration shared by Enqueue and
// Subscribe.
type QueueConfig struct {
	App       string
	Namespace string
}

// QueueOption configures a backend constructor.
type QueueOption interface {
	Apply(*QueueConfig)
}

// QueueOptionFunc adapts a function to QueueOption.
type QueueOptionFunc func(*QueueConfig)

// Apply calls f(cfg).
func (f QueueOptionFunc) Apply(cfg *QueueConfig) { f(cfg) }

// WithApp sets the service-level app name.
func WithApp(app string) QueueOption {
	return QueueOptionFunc(func(c *QueueConfig) {
		if app != "" {
			c.App = app
		}
	})
}

// WithNamespace sets the service-level namespace.
func WithNamespace(ns string) QueueOption {
	return QueueOptionFunc(func(c *QueueConfig) {
		if ns != "" {
			c.Namespace = ns
		}
	})
}

// ResolveSubscribeConfig fills in package defaults for any zero-valued field
// in cfg. Backend implementations call this once per Subscribe call after
// applying user options.
func ResolveSubscribeConfig(cfg *SubscribeConfig, formattedTopic string) {
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
	if cfg.Backoff == nil {
		cfg.Backoff = DefaultBackoff
	}
	if cfg.DeadLetterTopic == "" {
		cfg.DeadLetterTopic = DeadLetterTopic(formattedTopic)
	}
}
