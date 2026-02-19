package pgblob

import "fmt"

// CreateTableSQL returns the DDL for creating the blobs table.
func CreateTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	key         TEXT PRIMARY KEY,
	data        BYTEA NOT NULL,
	content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
	size        BIGINT NOT NULL DEFAULT 0,
	md5_hash    BYTEA,
	etag        TEXT,
	metadata    JSONB,
	created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_%s_key_prefix ON %s (key text_pattern_ops);`,
		tableName, tableName, tableName)
}
