package streambridge

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/enverbisevac/libs/outbox"
	osql "github.com/enverbisevac/libs/outbox/sql"
	outboxsqlite "github.com/enverbisevac/libs/outbox/sql/sqlite"
	"github.com/enverbisevac/libs/stream"
	streamsqlite "github.com/enverbisevac/libs/stream/sqlite"
)

// SQLiteConfig configures NewSQLite. The zero value is valid and produces
// the default outbox table name ("outbox_messages") and default backend
// configurations.
type SQLiteConfig struct {
	// Table is the outbox messages table name. Default "outbox_messages".
	Table string

	// StoreOpts are forwarded to outbox/sql/sqlite.New.
	StoreOpts []osql.Option

	// StreamOpts are forwarded to stream/sqlite.New.
	StreamOpts []stream.ServiceOption

	// RelayOpts are forwarded to outbox.NewRelay.
	RelayOpts []outbox.Option
}

// NewSQLite applies the outbox and stream schemas to db, constructs the
// Store and stream.Service over it, wires them through the streambridge
// Publisher, and returns a Bundle. The same db is shared between outbox
// and stream — each owns its own tables.
//
// The Bundle does not close db; the caller continues to own it.
func NewSQLite(ctx context.Context, db *sql.DB, cfg SQLiteConfig) (*Bundle, error) {
	if db == nil {
		return nil, fmt.Errorf("streambridge: NewSQLite: db is nil")
	}
	table := cfg.Table
	if table == "" {
		table = "outbox_messages"
	}

	if err := outboxsqlite.Migrate(ctx, db, table); err != nil {
		return nil, fmt.Errorf("streambridge: outbox migrate: %w", err)
	}
	if err := streamsqlite.Apply(ctx, db); err != nil {
		return nil, fmt.Errorf("streambridge: stream migrate: %w", err)
	}

	storeOpts := append([]osql.Option{osql.WithTableName(table)}, cfg.StoreOpts...)
	store := outboxsqlite.New(db, storeOpts...)
	svc := streamsqlite.New(db, cfg.StreamOpts...)

	return New(store, svc, cfg.RelayOpts...), nil
}
