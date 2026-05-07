package sqlite

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/enverbisevac/libs/outbox"
	osql "github.com/enverbisevac/libs/outbox/sql"
)

// DefaultProcessedTable is the default table name used by ProcessedStore
// when none is provided.
const DefaultProcessedTable = "processed_messages"

var _ outbox.ProcessedStore = (*ProcessedStore)(nil)

// ProcessedStore implements outbox.ProcessedStore for SQLite.
//
// Unlike Store, ProcessedStore does not hold a *sql.DB — every method
// takes the db/tx handle so callers compose with *sql.Tx for transactional
// exactly-once or pass *sql.DB for the cleanup loop.
type ProcessedStore struct {
	table string
}

// NewProcessed creates a ProcessedStore for SQLite. tableName defaults to
// DefaultProcessedTable when empty.
//
// SQLite has no schemas; pass an unqualified name. See sql.WithTableName
// for the equivalent constraint on the messages table.
func NewProcessed(tableName string) *ProcessedStore {
	if tableName == "" {
		tableName = DefaultProcessedTable
	}
	return &ProcessedStore{table: tableName}
}

// IsProcessed implements outbox.ProcessedStore. db must satisfy
// osql.DBTX (e.g. *sql.DB or *sql.Tx).
func (s *ProcessedStore) IsProcessed(ctx context.Context, db any, group, msgID string) (bool, error) {
	x, err := osql.AsDBTX(db)
	if err != nil {
		return false, err
	}
	query := fmt.Sprintf(
		`SELECT 1 FROM %s WHERE group_name = ? AND message_id = ?`,
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

// MarkProcessed implements outbox.ProcessedStore. Non-empty metadata must
// be valid JSON; invalid input is rejected before the INSERT runs.
//
// Validation matters for SQLite: the metadata column is TEXT (no JSON
// affinity), so without this check garbage would be silently stored.
func (s *ProcessedStore) MarkProcessed(ctx context.Context, db any, group, msgID string, metadata json.RawMessage) (bool, error) {
	if len(metadata) > 0 && !json.Valid(metadata) {
		return false, fmt.Errorf("outbox: mark processed: metadata is not valid JSON")
	}
	x, err := osql.AsDBTX(db)
	if err != nil {
		return false, err
	}
	query := fmt.Sprintf(
		`INSERT INTO %s (group_name, message_id, processed_at, metadata)
VALUES (?, ?, CURRENT_TIMESTAMP, ?)
ON CONFLICT (group_name, message_id) DO NOTHING`,
		s.table,
	)
	var meta any
	if len(metadata) > 0 {
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

// PurgeProcessedBefore implements outbox.ProcessedStore.
//
// SQLite stores DATETIME values as TEXT, and CURRENT_TIMESTAMP renders
// them as "YYYY-MM-DD HH:MM:SS" (UTC, space-separated, no Z). The default
// driver binding for time.Time uses a different shape (e.g.
// "2026-05-07T09:27:21Z"); a raw lexicographic comparison would mix the
// two and mis-order rows around the space-vs-'T' boundary. We format the
// cutoff to match CURRENT_TIMESTAMP's exact shape so the lex compare is
// also a chronological compare.
func (s *ProcessedStore) PurgeProcessedBefore(ctx context.Context, db any, t time.Time) (int64, error) {
	x, err := osql.AsDBTX(db)
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE processed_at < ?`,
		s.table,
	)
	cutoff := t.UTC().Format("2006-01-02 15:04:05")
	res, err := x.ExecContext(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("outbox: purge processed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("outbox: purge processed rows: %w", err)
	}
	return n, nil
}
