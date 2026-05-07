package postgres

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/enverbisevac/libs/outbox"
)

// DefaultProcessedTable is the default table name used by ProcessedStore
// when none is provided.
const DefaultProcessedTable = "processed_messages"

var _ outbox.ProcessedStore = (*ProcessedStore)(nil)

// dbtx is the database/sql shape this backend understands. *sql.DB,
// *sql.Tx and *sql.Conn satisfy it.
type dbtx interface {
	ExecContext(ctx context.Context, query string, args ...any) (stdsql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *stdsql.Row
}

func asDBTX(db any) (dbtx, error) {
	x, ok := db.(dbtx)
	if !ok {
		return nil, fmt.Errorf("outbox: unsupported db type %T (want *sql.DB or *sql.Tx)", db)
	}
	return x, nil
}

// ProcessedStore implements outbox.ProcessedStore for PostgreSQL.
//
// Unlike Store, ProcessedStore does not hold a *sql.DB — every method
// takes the db/tx handle so callers compose with *sql.Tx for transactional
// exactly-once or pass *sql.DB for the cleanup loop.
type ProcessedStore struct {
	table string
}

// NewProcessed creates a ProcessedStore for PostgreSQL. tableName defaults
// to DefaultProcessedTable when empty.
//
// Pass an unqualified name; set the schema via the connection's
// search_path. See sql.WithTableName for details.
func NewProcessed(tableName string) *ProcessedStore {
	if tableName == "" {
		tableName = DefaultProcessedTable
	}
	return &ProcessedStore{table: tableName}
}

func (s *ProcessedStore) IsProcessed(ctx context.Context, db any, group, msgID string) (bool, error) {
	x, err := asDBTX(db)
	if err != nil {
		return false, err
	}
	query := fmt.Sprintf(
		`SELECT 1 FROM %s WHERE group_name = $1 AND message_id = $2`,
		s.table,
	)
	var n int
	err = x.QueryRowContext(ctx, query, group, msgID).Scan(&n)
	if errors.Is(err, stdsql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("outbox: is processed: %w", err)
	}
	return true, nil
}

func (s *ProcessedStore) MarkProcessed(ctx context.Context, db any, group, msgID string, metadata []byte) (bool, error) {
	x, err := asDBTX(db)
	if err != nil {
		return false, err
	}
	query := fmt.Sprintf(
		`INSERT INTO %s (group_name, message_id, processed_at, metadata)
VALUES ($1, $2, now(), $3)
ON CONFLICT (group_name, message_id) DO NOTHING`,
		s.table,
	)
	var meta any
	if metadata != nil {
		meta = string(metadata)
	}
	res, err := x.ExecContext(ctx, query, group, msgID, meta)
	if err != nil {
		return false, fmt.Errorf("outbox: mark processed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("outbox: mark processed rows: %w", err)
	}
	return n > 0, nil
}

func (s *ProcessedStore) PurgeProcessedBefore(ctx context.Context, db any, t time.Time) (int64, error) {
	x, err := asDBTX(db)
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE processed_at < $1`,
		s.table,
	)
	res, err := x.ExecContext(ctx, query, t)
	if err != nil {
		return 0, fmt.Errorf("outbox: purge processed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("outbox: purge processed rows: %w", err)
	}
	return n, nil
}