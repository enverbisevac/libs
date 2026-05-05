package sqlite

import (
	"strings"
	"testing"
	"time"

	osql "github.com/enverbisevac/libs/outbox/sql"
)

func TestNewDefaults(t *testing.T) {
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
	if store.config.IDGenerator == nil {
		t.Error("IDGenerator should default to a non-nil function")
	}
}

func TestNewWithOptions(t *testing.T) {
	store := New(nil,
		osql.WithTableName("custom_outbox"),
		osql.WithMaxRetries(10),
		osql.WithRetryAfter(time.Minute),
	)

	if store.config.TableName != "custom_outbox" {
		t.Errorf("TableName = %q, want %q", store.config.TableName, "custom_outbox")
	}
	if store.config.MaxRetries != 10 {
		t.Errorf("MaxRetries = %d, want 10", store.config.MaxRetries)
	}
	if store.config.RetryAfter != time.Minute {
		t.Errorf("RetryAfter = %v, want %v", store.config.RetryAfter, time.Minute)
	}
}

func TestCreateTableSQL(t *testing.T) {
	sql := CreateTableSQL("outbox_messages")

	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS outbox_messages",
		"id TEXT PRIMARY KEY",
		"topic TEXT NOT NULL",
		"payload BLOB",
		"headers TEXT NOT NULL DEFAULT '{}'",
		"status TEXT NOT NULL DEFAULT 'pending'",
		"retries INTEGER NOT NULL DEFAULT 0",
		"created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
		"WHERE status = 'pending'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing fragment %q", want)
		}
	}
}

func TestCreateTableSQLCustomName(t *testing.T) {
	sql := CreateTableSQL("my_outbox")

	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS my_outbox",
		"idx_my_outbox_pending",
		"ON my_outbox",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing fragment %q", want)
		}
	}
}
