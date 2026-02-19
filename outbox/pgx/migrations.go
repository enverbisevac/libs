package pgx

import "fmt"

// CreateTableSQL returns the DDL for creating the outbox messages table.
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
