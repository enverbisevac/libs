package postgres

import (
	"context"
	"fmt"

	osql "github.com/enverbisevac/libs/outbox/sql"
)

// Migrate creates the outbox messages table if it does not exist.
// db can be a *sql.DB, *sql.Tx, *sql.Conn or any other type implementing
// osql.Execer.
func Migrate(ctx context.Context, db osql.Execer, tableName string) error {
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
	WHERE status = 'pending';`, tableName, tableName, tableName)
}
