// Package migrate dispatches outbox-table migrations to the right
// dialect based on a driver name string. Pulled into its own package so
// libs/outbox/sql can avoid importing its own subpackages.
//
// Driver mapping:
//   - "postgres", "pgx" → libs/outbox/sql/postgres
//   - everything else   → libs/outbox/sql/sqlite
//
// All migrations are idempotent (CREATE TABLE IF NOT EXISTS).
package migrate

import (
	"context"

	osql "github.com/enverbisevac/libs/outbox/sql"
	"github.com/enverbisevac/libs/outbox/sql/postgres"
	"github.com/enverbisevac/libs/outbox/sql/sqlite"
)

// SQL creates the outbox_messages table appropriate for driver. db can
// be a *sql.DB, *sql.Tx, *sql.Conn or any other type implementing
// osql.Execer.
func SQL(ctx context.Context, db osql.Execer, driver, tableName string) error {
	if isPostgres(driver) {
		return postgres.Migrate(ctx, db, tableName)
	}
	return sqlite.Migrate(ctx, db, tableName)
}

// isPostgres returns true for the canonical postgres driver names
// registered by lib/pq ("postgres") and jackc/pgx-stdlib ("pgx").
func isPostgres(driver string) bool {
	return driver == "postgres" || driver == "pgx"
}
