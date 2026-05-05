package pgx_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/enverbisevac/libs/internal/testdb"
	"github.com/enverbisevac/libs/stream"
	spgx "github.com/enverbisevac/libs/stream/pgx"
	"github.com/enverbisevac/libs/stream/streamtest"
)

// TestConformance uses STREAM_PGX_DSN if set, otherwise spins up an
// ephemeral postgres testcontainer. Skips if neither is available
// (e.g. Docker not running and no env var).
//
// The conformance suite's ConcurrencyNoDoubleDelivery case runs 4 workers +
// 4 publishers concurrently, plus the listener holds a permanent connection.
// We set MaxConns to 16 (well above the default of max(4, NumCPU)) so the
// pool doesn't deadlock on subscriber/publisher contention.
func TestConformance(t *testing.T) {
	dsn := testdb.Postgres(t, "STREAM_PGX_DSN")

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	if err := spgx.Apply(context.Background(), pool); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	streamtest.Run(t, func(t *testing.T) (stream.Service, func()) {
		// Each sub-test gets its own Service with a unique namespace
		// so durable state in the shared tables cannot bleed.
		svc := spgx.New(pool, stream.WithNamespace(t.Name()))
		return svc, func() {
			_ = svc.Close(context.Background())
		}
	})
}
