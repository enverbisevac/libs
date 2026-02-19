package pgx

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/enverbisevac/libs/outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ outbox.Store = (*Store)(nil)

// Store implements outbox.Store using PostgreSQL.
type Store struct {
	config Config
	pool   *pgxpool.Pool
	db     *sql.DB
}

// New creates a new outbox store using pgxpool.
func New(pool *pgxpool.Pool, options ...Option) *Store {
	config := Config{
		TableName:  "outbox_messages",
		MaxRetries: 5,
		RetryAfter: 30 * time.Second,
	}
	for _, opt := range options {
		opt.Apply(&config)
	}
	return &Store{
		config: config,
		pool:   pool,
	}
}

// NewStdLib creates a new outbox store using database/sql.
func NewStdLib(db *sql.DB, options ...Option) *Store {
	config := Config{
		TableName:  "outbox_messages",
		MaxRetries: 5,
		RetryAfter: 30 * time.Second,
	}
	for _, opt := range options {
		opt.Apply(&config)
	}
	return &Store{
		config: config,
		db:     db,
	}
}

// Save persists messages within the given transaction.
// tx can be pgx.Tx, *sql.Tx, or nil.
func (s *Store) Save(ctx context.Context, tx any, msgs ...outbox.Message) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (topic, key, payload, headers) VALUES ($1, $2, $3, $4)`,
		s.config.TableName,
	)

	switch t := tx.(type) {
	case pgx.Tx:
		for _, msg := range msgs {
			headers, err := json.Marshal(msg.Headers)
			if err != nil {
				return fmt.Errorf("outbox: marshal headers: %w", err)
			}
			if _, err := t.Exec(ctx, query, msg.Topic, msg.Key, msg.Payload, headers); err != nil {
				return fmt.Errorf("outbox: save: %w", err)
			}
		}
	case *sql.Tx:
		for _, msg := range msgs {
			headers, err := json.Marshal(msg.Headers)
			if err != nil {
				return fmt.Errorf("outbox: marshal headers: %w", err)
			}
			if _, err := t.ExecContext(ctx, query, msg.Topic, msg.Key, msg.Payload, headers); err != nil {
				return fmt.Errorf("outbox: save: %w", err)
			}
		}
	case nil:
		if s.pool != nil {
			return s.saveWithPool(ctx, query, msgs)
		}
		return s.saveWithDB(ctx, query, msgs)
	default:
		return fmt.Errorf("outbox: unsupported tx type %T", tx)
	}
	return nil
}

func (s *Store) saveWithPool(ctx context.Context, query string, msgs []outbox.Message) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("outbox: begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, msg := range msgs {
		headers, err := json.Marshal(msg.Headers)
		if err != nil {
			return fmt.Errorf("outbox: marshal headers: %w", err)
		}
		if _, err := tx.Exec(ctx, query, msg.Topic, msg.Key, msg.Payload, headers); err != nil {
			return fmt.Errorf("outbox: save: %w", err)
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) saveWithDB(ctx context.Context, query string, msgs []outbox.Message) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("outbox: begin tx: %w", err)
	}
	defer tx.Rollback()

	for _, msg := range msgs {
		headers, err := json.Marshal(msg.Headers)
		if err != nil {
			return fmt.Errorf("outbox: marshal headers: %w", err)
		}
		if _, err := tx.ExecContext(ctx, query, msg.Topic, msg.Key, msg.Payload, headers); err != nil {
			return fmt.Errorf("outbox: save: %w", err)
		}
	}
	return tx.Commit()
}

// FetchPending atomically claims up to limit pending messages for processing.
func (s *Store) FetchPending(ctx context.Context, limit int) ([]outbox.Message, error) {
	query := fmt.Sprintf(`UPDATE %s
SET status = 'processing'
WHERE id IN (
	SELECT id FROM %s
	WHERE status = 'pending'
	ORDER BY created_at
	FOR UPDATE SKIP LOCKED
	LIMIT $1
)
RETURNING id, topic, key, payload, headers, status, retries, last_error, created_at, processed_at`,
		s.config.TableName, s.config.TableName)

	if s.pool != nil {
		return s.fetchPendingPool(ctx, query, limit)
	}
	return s.fetchPendingDB(ctx, query, limit)
}

func (s *Store) fetchPendingPool(ctx context.Context, query string, limit int) ([]outbox.Message, error) {
	rows, err := s.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox: fetch pending: %w", err)
	}
	defer rows.Close()
	return scanMessages(rows)
}

func (s *Store) fetchPendingDB(ctx context.Context, query string, limit int) ([]outbox.Message, error) {
	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox: fetch pending: %w", err)
	}
	defer rows.Close()
	return scanSQLMessages(rows)
}

// MarkProcessed marks messages as successfully processed.
func (s *Store) MarkProcessed(ctx context.Context, ids ...string) error {
	query := fmt.Sprintf(
		`UPDATE %s SET status = 'processed', processed_at = now() WHERE id = ANY($1)`,
		s.config.TableName,
	)

	if s.pool != nil {
		if _, err := s.pool.Exec(ctx, query, ids); err != nil {
			return fmt.Errorf("outbox: mark processed: %w", err)
		}
	} else {
		if _, err := s.db.ExecContext(ctx, query, pgArrayUUID(ids)); err != nil {
			return fmt.Errorf("outbox: mark processed: %w", err)
		}
	}
	return nil
}

// MarkFailed increments retries and records the error.
func (s *Store) MarkFailed(ctx context.Context, id string, err error) error {
	query := fmt.Sprintf(
		`UPDATE %s SET
			retries = retries + 1,
			last_error = $1,
			status = CASE WHEN retries + 1 >= $2 THEN 'failed' ELSE 'pending' END
		WHERE id = $3`,
		s.config.TableName,
	)

	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}

	if s.pool != nil {
		if _, execErr := s.pool.Exec(ctx, query, errMsg, s.config.MaxRetries, id); execErr != nil {
			return fmt.Errorf("outbox: mark failed: %w", execErr)
		}
	} else {
		if _, execErr := s.db.ExecContext(ctx, query, errMsg, s.config.MaxRetries, id); execErr != nil {
			return fmt.Errorf("outbox: mark failed: %w", execErr)
		}
	}
	return nil
}

func scanMessages(rows pgx.Rows) ([]outbox.Message, error) {
	var msgs []outbox.Message
	for rows.Next() {
		var msg outbox.Message
		var headers []byte
		var processedAt *time.Time
		if err := rows.Scan(
			&msg.ID, &msg.Topic, &msg.Key, &msg.Payload,
			&headers, &msg.Status, &msg.Retries, &msg.LastError,
			&msg.CreatedAt, &processedAt,
		); err != nil {
			return nil, fmt.Errorf("outbox: scan: %w", err)
		}
		if processedAt != nil {
			msg.ProcessedAt = *processedAt
		}
		if len(headers) > 0 {
			if err := json.Unmarshal(headers, &msg.Headers); err != nil {
				return nil, fmt.Errorf("outbox: unmarshal headers: %w", err)
			}
		}
		msgs = append(msgs, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: rows: %w", err)
	}
	return msgs, nil
}

func scanSQLMessages(rows *sql.Rows) ([]outbox.Message, error) {
	var msgs []outbox.Message
	for rows.Next() {
		var msg outbox.Message
		var headers []byte
		var processedAt *time.Time
		if err := rows.Scan(
			&msg.ID, &msg.Topic, &msg.Key, &msg.Payload,
			&headers, &msg.Status, &msg.Retries, &msg.LastError,
			&msg.CreatedAt, &processedAt,
		); err != nil {
			return nil, fmt.Errorf("outbox: scan: %w", err)
		}
		if processedAt != nil {
			msg.ProcessedAt = *processedAt
		}
		if len(headers) > 0 {
			if err := json.Unmarshal(headers, &msg.Headers); err != nil {
				return nil, fmt.Errorf("outbox: unmarshal headers: %w", err)
			}
		}
		msgs = append(msgs, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: rows: %w", err)
	}
	return msgs, nil
}

// pgArrayUUID wraps a string slice for use with database/sql ANY($1) queries.
type pgArrayUUID []string

func (a pgArrayUUID) Value() (any, error) {
	return []string(a), nil
}
