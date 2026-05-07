package postgres

import (
	"context"
	"fmt"

	osql "github.com/enverbisevac/libs/outbox/sql"
)

// Migrate creates the outbox messages table if it does not exist.
// db can be a *sql.DB, *sql.Tx, *sql.Conn or any other type implementing
// osql.Execer. tableName defaults to "outbox_messages" when empty (matches
// sql.DefaultConfig and MigrateProcessed's symmetric default).
func Migrate(ctx context.Context, db osql.Execer, tableName string) error {
	if tableName == "" {
		tableName = "outbox_messages"
	}
	if _, err := db.ExecContext(ctx, CreateTableSQL(tableName)); err != nil {
		return fmt.Errorf("outbox: migrate: %w", err)
	}
	return nil
}

// CreateTableSQL returns the PostgreSQL DDL for creating the outbox messages table.
func CreateTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	topic TEXT NOT NULL,
	key TEXT NOT NULL DEFAULT '',
	payload BYTEA,
	headers JSONB DEFAULT '{}',
	status TEXT NOT NULL DEFAULT 'pending',
	retries INTEGER NOT NULL DEFAULT 0,
	last_error TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_%s_pending
	ON %s (created_at)
	WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_%s_processed_at
	ON %s (processed_at)
	WHERE status = 'processed';`, tableName, tableName, tableName, tableName, tableName)
}

// MigrateProcessed creates the processed_messages table if it does not
// exist. db can be a *sql.DB, *sql.Tx, *sql.Conn or any other type
// implementing osql.Execer. tableName defaults to DefaultProcessedTable
// when empty.
func MigrateProcessed(ctx context.Context, db osql.Execer, tableName string) error {
	if tableName == "" {
		tableName = DefaultProcessedTable
	}
	if _, err := db.ExecContext(ctx, CreateProcessedTableSQL(tableName)); err != nil {
		return fmt.Errorf("outbox: migrate processed: %w", err)
	}
	return nil
}

// CreateProcessedTableSQL returns the PostgreSQL DDL for creating the
// processed_messages dedup table used by outbox.ProcessedStore.
//
// metadata is JSONB so callers can attach handler-specific context
// (printed printer names, config snapshots, audit trails) without further
// migrations.
func CreateProcessedTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	group_name   TEXT NOT NULL,
	message_id   TEXT NOT NULL,
	processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	metadata     JSONB,
	PRIMARY KEY (group_name, message_id)
);

CREATE INDEX IF NOT EXISTS idx_%s_processed_at
	ON %s (processed_at);`, tableName, tableName, tableName)
}
