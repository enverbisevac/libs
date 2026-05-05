package streambridge

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/enverbisevac/libs/outbox"
	osql "github.com/enverbisevac/libs/outbox/sql"
	outboxpostgres "github.com/enverbisevac/libs/outbox/sql/postgres"
	"github.com/enverbisevac/libs/stream"
	streampgx "github.com/enverbisevac/libs/stream/pgx"
)

// PostgresConfig configures NewPostgres. The zero value is valid.
type PostgresConfig struct {
	// Table is the outbox messages table name. Default "outbox_messages".
	Table string

	// StoreOpts are forwarded to outbox/sql/postgres.New.
	StoreOpts []osql.Option

	// StreamOpts are forwarded to stream/pgx.New.
	StreamOpts []stream.ServiceOption

	// RelayOpts are forwarded to outbox.NewRelay.
	RelayOpts []outbox.Option
}

// NewPostgres builds a Bundle from a pgx pool. The pool drives stream/pgx
// directly; for outbox a *sql.DB is derived via stdlib.OpenDBFromPool so
// the existing database/sql-based outbox.Store can use the same connection
// pool. Schemas for both are applied before returning.
//
// Bundle.Stop closes the derived *sql.DB but does NOT close the pool —
// the caller continues to own it.
func NewPostgres(ctx context.Context, pool *pgxpool.Pool, cfg PostgresConfig) (*Bundle, error) {
	if pool == nil {
		return nil, fmt.Errorf("streambridge: NewPostgres: pool is nil")
	}
	table := cfg.Table
	if table == "" {
		table = "outbox_messages"
	}

	db := stdlib.OpenDBFromPool(pool)
	// On any error past this point we must release the derived *sql.DB.
	cleanup := func() {
		_ = db.Close()
	}

	if err := outboxpostgres.Migrate(ctx, db, table); err != nil {
		cleanup()
		return nil, fmt.Errorf("streambridge: outbox migrate: %w", err)
	}
	if err := streampgx.Apply(ctx, pool); err != nil {
		cleanup()
		return nil, fmt.Errorf("streambridge: stream migrate: %w", err)
	}

	storeOpts := append([]osql.Option{osql.WithTableName(table)}, cfg.StoreOpts...)
	store := outboxpostgres.New(db, storeOpts...)
	svc := streampgx.New(pool, cfg.StreamOpts...)

	b := New(store, svc, cfg.RelayOpts...)
	b.closers = append(b.closers, db.Close)
	return b, nil
}
