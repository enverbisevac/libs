package queue_test

import (
	"testing"
	"time"

	"github.com/enverbisevac/libs/queue"
)

func TestEnqueueOptionsApply(t *testing.T) {
	cfg := queue.EnqueueConfig{
		App:       queue.DefaultAppName,
		Namespace: queue.DefaultNamespace,
	}
	now := time.Now()
	opts := []queue.EnqueueOption{
		queue.WithDelay(5 * time.Second),
		queue.WithPriority(queue.PriorityHigh),
		queue.WithEnqueueApp("svc"),
		queue.WithEnqueueNamespace("prod"),
	}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	if cfg.App != "svc" || cfg.Namespace != "prod" {
		t.Fatalf("app/ns not applied: %+v", cfg)
	}
	if cfg.Priority != queue.PriorityHigh {
		t.Fatalf("priority not applied")
	}
	if cfg.RunAt.IsZero() || cfg.RunAt.Before(now.Add(4*time.Second)) {
		t.Fatalf("WithDelay did not set RunAt")
	}
}

func TestWithRunAtOverridesDelay(t *testing.T) {
	cfg := queue.EnqueueConfig{}
	at := time.Now().Add(time.Hour)
	queue.WithDelay(5 * time.Second).Apply(&cfg)
	queue.WithRunAt(at).Apply(&cfg)
	if !cfg.RunAt.Equal(at) {
		t.Fatalf("WithRunAt did not override WithDelay")
	}
}

func TestSubscribeOptionsApply(t *testing.T) {
	cfg := queue.SubscribeConfig{
		MaxRetries:        queue.DefaultMaxRetries,
		PollInterval:      queue.DefaultPollInterval,
		VisibilityTimeout: queue.DefaultVisibilityTimeout,
		ShutdownTimeout:   queue.DefaultShutdownTimeout,
		Concurrency:       1,
	}
	queue.WithConcurrency(8).Apply(&cfg)
	queue.WithMaxRetries(10).Apply(&cfg)
	queue.WithPollInterval(2 * time.Second).Apply(&cfg)
	queue.WithVisibilityTimeout(2 * time.Minute).Apply(&cfg)
	queue.WithShutdownTimeout(time.Minute).Apply(&cfg)
	queue.WithDeadLetterTopic("custom.dlq").Apply(&cfg)
	queue.WithBackoff(queue.DefaultBackoff).Apply(&cfg)

	if cfg.Concurrency != 8 {
		t.Fatalf("concurrency = %d, want 8", cfg.Concurrency)
	}
	if cfg.MaxRetries != 10 {
		t.Fatalf("maxretries = %d, want 10", cfg.MaxRetries)
	}
	if cfg.PollInterval != 2*time.Second {
		t.Fatalf("poll = %v, want 2s", cfg.PollInterval)
	}
	if cfg.VisibilityTimeout != 2*time.Minute {
		t.Fatalf("vis = %v", cfg.VisibilityTimeout)
	}
	if cfg.ShutdownTimeout != time.Minute {
		t.Fatalf("shutdown = %v", cfg.ShutdownTimeout)
	}
	if cfg.DeadLetterTopic != "custom.dlq" {
		t.Fatalf("dlq = %q", cfg.DeadLetterTopic)
	}
	if cfg.Backoff == nil {
		t.Fatalf("backoff is nil")
	}
}
