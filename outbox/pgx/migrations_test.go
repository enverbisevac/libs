package pgx

import (
	"strings"
	"testing"
)

func TestCreateTableSQL(t *testing.T) {
	sql := CreateTableSQL("outbox_messages")

	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS outbox_messages") {
		t.Error("missing CREATE TABLE statement")
	}
	if !strings.Contains(sql, "id UUID PRIMARY KEY DEFAULT gen_random_uuid()") {
		t.Error("missing UUID primary key")
	}
	if !strings.Contains(sql, "topic TEXT NOT NULL") {
		t.Error("missing topic column")
	}
	if !strings.Contains(sql, "payload BYTEA") {
		t.Error("missing payload column")
	}
	if !strings.Contains(sql, "headers JSONB") {
		t.Error("missing headers column")
	}
	if !strings.Contains(sql, "status TEXT NOT NULL DEFAULT 'pending'") {
		t.Error("missing status column")
	}
	if !strings.Contains(sql, "retries INTEGER NOT NULL DEFAULT 0") {
		t.Error("missing retries column")
	}
	if !strings.Contains(sql, "WHERE status = 'pending'") {
		t.Error("missing partial index on pending status")
	}
}

func TestCreateTableSQLCustomName(t *testing.T) {
	sql := CreateTableSQL("my_outbox")

	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS my_outbox") {
		t.Error("should use custom table name in CREATE TABLE")
	}
	if !strings.Contains(sql, "idx_my_outbox_pending") {
		t.Error("should use custom table name in index name")
	}
	if !strings.Contains(sql, "ON my_outbox") {
		t.Error("should use custom table name in index definition")
	}
}
