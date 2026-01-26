package pgx

import (
	"context"
	"database/sql"
	"hash/fnv"
	"sync"

	"github.com/enverbisevac/libs/lock"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// Service implements lock.Service using PostgreSQL advisory locks.
type Service struct {
	config Config
	pool   *pgxpool.Pool
	db     *sql.DB
}

// New creates a new lock service using pgxpool.
func New(pool *pgxpool.Pool, options ...Option) *Service {
	config := Config{
		Namespace: 0,
	}
	for _, opt := range options {
		opt.Apply(&config)
	}

	return &Service{
		config: config,
		pool:   pool,
	}
}

// NewStdLib creates a new lock service using database/sql.
func NewStdLib(db *sql.DB, options ...Option) *Service {
	config := Config{
		Namespace: 0,
	}
	for _, opt := range options {
		opt.Apply(&config)
	}

	return &Service{
		config: config,
		db:     db,
	}
}

// NewLock creates a new lock with the given key.
func (s *Service) NewLock(key string) lock.Locker {
	return &locker{
		service: s,
		key:     hashKey(key),
	}
}

// hashKey converts a string key to int32 using FNV-1a hash.
func hashKey(key string) int32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int32(h.Sum32())
}

type locker struct {
	service *Service
	key     int32

	mu       sync.Mutex
	conn     *pgx.Conn
	poolConn *pgxpool.Conn
	dbConn   *sql.Conn
	locked   bool
}

// Lock acquires the lock, blocking until it's available or context is cancelled.
func (l *locker) Lock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.locked {
		return nil
	}

	if err := l.acquireConn(ctx); err != nil {
		return err
	}

	if l.conn != nil {
		_, err := l.conn.Exec(ctx, "SELECT pg_advisory_lock($1, $2)", l.service.config.Namespace, l.key)
		if err != nil {
			l.releaseConn()
			return err
		}
	} else if l.dbConn != nil {
		_, err := l.dbConn.ExecContext(ctx, "SELECT pg_advisory_lock($1, $2)", l.service.config.Namespace, l.key)
		if err != nil {
			l.releaseConn()
			return err
		}
	}

	l.locked = true
	return nil
}

// TryLock attempts to acquire the lock without blocking.
func (l *locker) TryLock(ctx context.Context) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.locked {
		return true, nil
	}

	if err := l.acquireConn(ctx); err != nil {
		return false, err
	}

	var acquired bool
	if l.conn != nil {
		err := l.conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1, $2)", l.service.config.Namespace, l.key).Scan(&acquired)
		if err != nil {
			l.releaseConn()
			return false, err
		}
	} else if l.dbConn != nil {
		err := l.dbConn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1, $2)", l.service.config.Namespace, l.key).Scan(&acquired)
		if err != nil {
			l.releaseConn()
			return false, err
		}
	}

	if !acquired {
		l.releaseConn()
		return false, nil
	}

	l.locked = true
	return true, nil
}

// Unlock releases the lock.
func (l *locker) Unlock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.locked {
		return nil
	}

	if l.conn != nil {
		_, err := l.conn.Exec(ctx, "SELECT pg_advisory_unlock($1, $2)", l.service.config.Namespace, l.key)
		if err != nil {
			return err
		}
	} else if l.dbConn != nil {
		_, err := l.dbConn.ExecContext(ctx, "SELECT pg_advisory_unlock($1, $2)", l.service.config.Namespace, l.key)
		if err != nil {
			return err
		}
	}

	l.locked = false
	l.releaseConn()
	return nil
}

func (l *locker) acquireConn(ctx context.Context) error {
	if l.conn != nil || l.dbConn != nil {
		return nil
	}

	if l.service.pool != nil {
		poolConn, err := l.service.pool.Acquire(ctx)
		if err != nil {
			return err
		}
		l.poolConn = poolConn
		l.conn = poolConn.Conn()
	} else if l.service.db != nil {
		dbConn, err := l.service.db.Conn(ctx)
		if err != nil {
			return err
		}
		var conn *pgx.Conn
		err = dbConn.Raw(func(driverConn any) error {
			conn = driverConn.(*stdlib.Conn).Conn()
			return nil
		})
		if err != nil {
			dbConn.Close()
			return err
		}
		l.dbConn = dbConn
		l.conn = conn
	}

	return nil
}

func (l *locker) releaseConn() {
	if l.poolConn != nil {
		l.poolConn.Release()
		l.poolConn = nil
		l.conn = nil
	}
	if l.dbConn != nil {
		l.dbConn.Close()
		l.dbConn = nil
		l.conn = nil
	}
}