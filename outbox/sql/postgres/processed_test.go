package postgres

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestNewProcessedDefaultTable(t *testing.T) {
	if got := NewProcessed("").table; got != DefaultProcessedTable {
		t.Errorf("table = %q, want %q", got, DefaultProcessedTable)
	}
	if got := NewProcessed("custom").table; got != "custom" {
		t.Errorf("table = %q, want %q", got, "custom")
	}
}

func TestCreateProcessedTableSQL(t *testing.T) {
	s := CreateProcessedTableSQL("processed_messages")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS processed_messages",
		"group_name   TEXT NOT NULL",
		"message_id   TEXT NOT NULL",
		"processed_at TIMESTAMPTZ NOT NULL DEFAULT now()",
		"metadata     JSONB",
		"PRIMARY KEY (group_name, message_id)",
		"idx_processed_messages_processed_at",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("missing fragment %q in DDL", want)
		}
	}
}

func TestCreateProcessedTableSQLCustomName(t *testing.T) {
	s := CreateProcessedTableSQL("my_processed")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS my_processed",
		"idx_my_processed_processed_at",
		"ON my_processed",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("missing fragment %q in DDL", want)
		}
	}
}

func TestProcessedStore_RejectsBadDBHandle(t *testing.T) {
	s := NewProcessed("")
	type wrong struct{}

	if _, err := s.IsProcessed(context.Background(), wrong{}, "g", "m"); err == nil {
		t.Error("IsProcessed: expected error for unsupported db type")
	}
	if _, err := s.MarkProcessed(context.Background(), wrong{}, "g", "m", nil); err == nil {
		t.Error("MarkProcessed: expected error for unsupported db type")
	}
	if _, err := s.PurgeProcessedBefore(context.Background(), wrong{}, time.Now()); err == nil {
		t.Error("PurgeProcessedBefore: expected error for unsupported db type")
	}
}

func TestProcessedStore_RejectsInvalidJSONMetadata(t *testing.T) {
	s := NewProcessed("")

	// Validation runs before AsDBTX, so a nil db is fine here — the test
	// fails fast on the JSON check without touching the database.
	_, err := s.MarkProcessed(context.Background(), nil, "g", "m", []byte("not json"))
	if err == nil || !strings.Contains(err.Error(), "not valid JSON") {
		t.Errorf("expected 'not valid JSON' error, got %v", err)
	}
}
