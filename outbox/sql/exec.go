package sql

import (
	"context"
	"database/sql"
)

// Execer is the minimal interface needed to execute a statement.
// Both *sql.DB, *sql.Tx and *sql.Conn satisfy it.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}
