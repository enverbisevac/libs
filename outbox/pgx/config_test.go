package pgx

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	store := New(nil)

	if store.config.TableName != "outbox_messages" {
		t.Errorf("TableName = %q, want %q", store.config.TableName, "outbox_messages")
	}
	if store.config.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, want 5", store.config.MaxRetries)
	}
	if store.config.RetryAfter != 30*time.Second {
		t.Errorf("RetryAfter = %v, want %v", store.config.RetryAfter, 30*time.Second)
	}
}

func TestWithTableName(t *testing.T) {
	store := New(nil, WithTableName("custom_outbox"))

	if store.config.TableName != "custom_outbox" {
		t.Errorf("TableName = %q, want %q", store.config.TableName, "custom_outbox")
	}
}

func TestWithTableNameEmpty(t *testing.T) {
	store := New(nil, WithTableName(""))

	if store.config.TableName != "outbox_messages" {
		t.Errorf("TableName = %q, want %q (should keep default for empty string)", store.config.TableName, "outbox_messages")
	}
}

func TestWithMaxRetries(t *testing.T) {
	store := New(nil, WithMaxRetries(10))

	if store.config.MaxRetries != 10 {
		t.Errorf("MaxRetries = %d, want 10", store.config.MaxRetries)
	}
}

func TestWithRetryAfter(t *testing.T) {
	store := New(nil, WithRetryAfter(time.Minute))

	if store.config.RetryAfter != time.Minute {
		t.Errorf("RetryAfter = %v, want %v", store.config.RetryAfter, time.Minute)
	}
}
