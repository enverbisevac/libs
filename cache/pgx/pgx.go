package pgx

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/enverbisevac/libs/cache"
	"github.com/enverbisevac/libs/sqlutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound             = errors.New("key not found")
	DefaultOperationTimeout = 10 * time.Second
	DefaultCleanupInterval  = 5 * time.Minute
	DefaultTableName        = "cache_entries"
)

var _ cache.Cache = (*Cache)(nil)

// pgxRowsAdapter wraps pgx.Rows to implement sqlutil.Rows interface
type pgxRowsAdapter struct {
	pgx.Rows
}

func (r pgxRowsAdapter) Close() error {
	r.Rows.Close()
	return nil
}

func isNotFound(err error) bool {
	return errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows)
}

type Cache struct {
	pool      *pgxpool.Pool
	db        *sql.DB
	tableName string

	mu     sync.RWMutex
	closed bool
	done   chan struct{}
	cancel context.CancelFunc
}

func (c *Cache) exec(ctx context.Context, query string, args ...any) error {
	if c.pool != nil {
		_, err := c.pool.Exec(ctx, query, args...)
		return err
	}
	_, err := c.db.ExecContext(ctx, query, args...)
	return err
}

func (c *Cache) queryRow(ctx context.Context, query string, args ...any) sqlutil.Scannable {
	if c.pool != nil {
		return c.pool.QueryRow(ctx, query, args...)
	}
	return c.db.QueryRowContext(ctx, query, args...)
}

func (c *Cache) query(ctx context.Context, query string, args ...any) (sqlutil.Rows, error) {
	if c.pool != nil {
		rows, err := c.pool.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return pgxRowsAdapter{rows}, nil
	}
	return c.db.QueryContext(ctx, query, args...)
}

type Option func(*Cache)

func WithTableName(name string) Option {
	return func(c *Cache) {
		c.tableName = name
	}
}

func New(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (*Cache, error) {
	c := &Cache{
		pool:      pool,
		tableName: DefaultTableName,
		done:      make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	if err := c.createTable(ctx); err != nil {
		return nil, err
	}

	c.startCleanup()

	return c, nil
}

func NewStdLib(ctx context.Context, db *sql.DB, opts ...Option) (*Cache, error) {
	c := &Cache{
		db:        db,
		tableName: DefaultTableName,
		done:      make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	if err := c.createTable(ctx); err != nil {
		return nil, err
	}

	c.startCleanup()

	return c, nil
}

func (c *Cache) createTable(ctx context.Context) error {
	tableQuery := `CREATE UNLOGGED TABLE IF NOT EXISTS ` + c.tableName + ` (
		key TEXT PRIMARY KEY,
		value BYTEA NOT NULL,
		expiry TIMESTAMPTZ NOT NULL
	)`
	indexQuery := `CREATE INDEX IF NOT EXISTS ` + c.tableName + `_expiry_idx ON ` + c.tableName + ` (expiry)`

	if err := c.exec(ctx, tableQuery); err != nil {
		return err
	}
	return c.exec(ctx, indexQuery)
}

func (c *Cache) startCleanup() {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	go func() {
		ticker := time.NewTicker(DefaultCleanupInterval)
		defer ticker.Stop()
		defer close(c.done)

		for {
			select {
			case <-ticker.C:
				c.cleanup()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *Cache) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	_ = c.exec(ctx, `DELETE FROM `+c.tableName+` WHERE expiry < NOW()`)
}

func (c *Cache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	if c.cancel != nil {
		c.cancel()
		<-c.done
	}

	return nil
}

func (c *Cache) Set(key string, value any, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&value); err != nil {
		return err
	}

	query := `INSERT INTO ` + c.tableName + ` (key, value, expiry)
		VALUES ($1, $2, $3)
		ON CONFLICT (key) DO UPDATE SET value = $2, expiry = $3`

	return c.exec(ctx, query, key, buf.Bytes(), time.Now().Add(ttl))
}

func (c *Cache) Get(key string) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	var data []byte
	var expiry time.Time

	query := `SELECT value, expiry FROM ` + c.tableName + ` WHERE key = $1`
	if err := c.queryRow(ctx, query, key).Scan(&data, &expiry); err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if time.Now().After(expiry) {
		_ = c.Remove(key)
		return nil, ErrNotFound
	}

	var value any
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&value); err != nil {
		return nil, err
	}

	return value, nil
}

func (c *Cache) Remove(keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	placeholders := make([]string, len(keys))
	args := make([]any, len(keys))
	for i, key := range keys {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = key
	}

	query := `DELETE FROM ` + c.tableName + ` WHERE key IN (` + strings.Join(placeholders, ",") + `)`
	return c.exec(ctx, query, args...)
}

func (c *Cache) Pop(key string) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	var data []byte
	var expiry time.Time

	query := `DELETE FROM ` + c.tableName + ` WHERE key = $1 RETURNING value, expiry`
	if err := c.queryRow(ctx, query, key).Scan(&data, &expiry); err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if time.Now().After(expiry) {
		return nil, ErrNotFound
	}

	var value any
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&value); err != nil {
		return nil, err
	}

	return value, nil
}

func (c *Cache) Keys(prefix string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	query := `SELECT key FROM ` + c.tableName + ` WHERE key LIKE $1 AND expiry > NOW()`
	rows, err := c.query(ctx, query, prefix+"%")
	if err != nil {
		return []string{}
	}

	var keys []string
	_ = sqlutil.ScanRows(rows, func(row sqlutil.Scannable) error {
		var key string
		if err := row.Scan(&key); err != nil {
			return err
		}
		keys = append(keys, key)
		return nil
	})

	return keys
}
