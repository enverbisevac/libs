package postgres

import (
	"context"
	"database/sql"
	"fmt"

	osql "github.com/enverbisevac/libs/outbox/sql"
)

// Migrate creates the outbox messages table if it does not exist.
// db can be a *sql.DB, *sql.Tx, *sql.Conn or any other type implementing
// osql.Execer. tableName defaults to "outbox_messages" when empty (matches
// sql.DefaultConfig and MigrateProcessed's symmetric default).
//
// Boot-fast path: when db also satisfies osql.DBTX (it does for *sql.DB
// / *sql.Tx / *sql.Conn), a single to_regclass($1) probe checks whether
// the canonical table already exists. The DDL itself is idempotent but
// CREATE ... IF NOT EXISTS still does catalog work + pg_class locking
// on every call, which adds up across the libraries that run on every
// process start. to_regclass is a plain catalog read with no locking.
//
// Schema-monotonicity assumption: if the table exists we skip every
// statement, so new indexes/columns added in a later release will not
// be applied to existing installs by this function. When the schema
// evolves, ship the change through a separately named migration call
// (e.g. MigrateV2) and invoke both at startup.
func Migrate(ctx context.Context, db osql.Execer, tableName string) error {
	if tableName == "" {
		tableName = "outbox_messages"
	}
	if exists, err := tableExists(ctx, db, tableName); err == nil && exists {
		return nil
	}
	if _, err := db.ExecContext(ctx, CreateTableSQL(tableName)); err != nil {
		return fmt.Errorf("outbox: migrate: %w", err)
	}
	return nil
}

// tableExists asks Postgres whether name resolves to a relation in the
// current search_path. When db cannot satisfy DBTX (caller passed a
// pure Execer) we return (false, nil) so the caller falls back to its
// idempotent DDL path — the optimization is best-effort.
func tableExists(ctx context.Context, db osql.Execer, name string) (bool, error) {
	dbtx, ok := db.(osql.DBTX)
	if !ok {
		return false, nil
	}
	var oid sql.NullInt64
	if err := dbtx.QueryRowContext(ctx, `SELECT to_regclass($1)::oid`, name).Scan(&oid); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return oid.Valid, nil
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
// when empty. Shares the to_regclass fast-path documented on Migrate;
// the same monotonic-schema caveat applies.
func MigrateProcessed(ctx context.Context, db osql.Execer, tableName string) error {
	if tableName == "" {
		tableName = DefaultProcessedTable
	}
	if exists, err := tableExists(ctx, db, tableName); err == nil && exists {
		return nil
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
