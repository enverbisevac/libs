package sql

import (
	"context"
	"database/sql"
	"fmt"
)

// Execer is the minimal interface needed to execute a statement.
// Both *sql.DB, *sql.Tx and *sql.Conn satisfy it.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// DBTX is the minimal database/sql interface needed for both reads and
// writes. *sql.DB, *sql.Tx and *sql.Conn all satisfy it.
//
// The outbox.ProcessedStore implementations in outbox/sql/postgres and
// outbox/sql/sqlite accept any value satisfying DBTX so callers can pass a
// *sql.Tx for transactional exactly-once or *sql.DB for read-only checks
// and the cleanup loop.
type DBTX interface {
	Execer
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// AsDBTX type-asserts db to a DBTX. Used by sql-based ProcessedStore
// implementations whose interface methods take db as any.
func AsDBTX(db any) (DBTX, error) {
	x, ok := db.(DBTX)
	if !ok {
		return nil, fmt.Errorf("outbox: unsupported db type %T (want *sql.DB, *sql.Tx or *sql.Conn)", db)
	}
	return x, nil
}
