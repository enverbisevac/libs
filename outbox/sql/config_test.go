package sql

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()

	if c.TableName != "outbox_messages" {
		t.Errorf("TableName = %q, want %q", c.TableName, "outbox_messages")
	}
	if c.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, want 5", c.MaxRetries)
	}
	if c.RetryAfter != 30*time.Second {
		t.Errorf("RetryAfter = %v, want %v", c.RetryAfter, 30*time.Second)
	}
}

func TestWithTableName(t *testing.T) {
	c := DefaultConfig()
	WithTableName("custom_outbox").Apply(&c)

	if c.TableName != "custom_outbox" {
		t.Errorf("TableName = %q, want %q", c.TableName, "custom_outbox")
	}
}

func TestWithTableNameEmpty(t *testing.T) {
	c := DefaultConfig()
	WithTableName("").Apply(&c)

	if c.TableName != "outbox_messages" {
		t.Errorf("TableName = %q, want %q (should keep default for empty string)", c.TableName, "outbox_messages")
	}
}

func TestWithMaxRetries(t *testing.T) {
	c := DefaultConfig()
	WithMaxRetries(10).Apply(&c)

	if c.MaxRetries != 10 {
		t.Errorf("MaxRetries = %d, want 10", c.MaxRetries)
	}
}

func TestWithRetryAfter(t *testing.T) {
	c := DefaultConfig()
	WithRetryAfter(time.Minute).Apply(&c)

	if c.RetryAfter != time.Minute {
		t.Errorf("RetryAfter = %v, want %v", c.RetryAfter, time.Minute)
	}
}
