package queue_test

import (
	"testing"
	"time"

	"github.com/enverbisevac/libs/queue"
)

func TestDefaults(t *testing.T) {
	if queue.DefaultAppName != "app" {
		t.Fatalf("DefaultAppName = %q, want %q", queue.DefaultAppName, "app")
	}
	if queue.DefaultNamespace != "default" {
		t.Fatalf("DefaultNamespace = %q, want %q", queue.DefaultNamespace, "default")
	}
	if queue.DefaultMaxRetries != 3 {
		t.Fatalf("DefaultMaxRetries = %d, want 3", queue.DefaultMaxRetries)
	}
	if queue.DefaultPollInterval != time.Second {
		t.Fatalf("DefaultPollInterval = %v, want 1s", queue.DefaultPollInterval)
	}
	if queue.DefaultVisibilityTimeout != 30*time.Second {
		t.Fatalf("DefaultVisibilityTimeout = %v, want 30s", queue.DefaultVisibilityTimeout)
	}
	if queue.DefaultShutdownTimeout != 30*time.Second {
		t.Fatalf("DefaultShutdownTimeout = %v, want 30s", queue.DefaultShutdownTimeout)
	}
}

func TestFormatTopic(t *testing.T) {
	got := queue.FormatTopic("svc", "prod", "emails")
	if got != "svc.prod.emails" {
		t.Fatalf("FormatTopic = %q, want %q", got, "svc.prod.emails")
	}
}

func TestDeadLetterTopic(t *testing.T) {
	got := queue.DeadLetterTopic("svc.prod.emails")
	if got != "svc.prod.emails.dead" {
		t.Fatalf("DeadLetterTopic = %q, want %q", got, "svc.prod.emails.dead")
	}
}
