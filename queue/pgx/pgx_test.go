package pgx_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/enverbisevac/libs/queue"
	qpgx "github.com/enverbisevac/libs/queue/pgx"
	"github.com/enverbisevac/libs/queue/queuetest"
)

// QUEUE_PGX_DSN must be set to a Postgres DSN that allows DDL. Example:
//
//	QUEUE_PGX_DSN="postgres://postgres:postgres@localhost:5432/queue_test?sslmode=disable"
//
// The test creates and drops the queue_jobs table for each conformance run.
func TestConformance(t *testing.T) {
	dsn := os.Getenv("QUEUE_PGX_DSN")
	if dsn == "" {
		t.Skip("QUEUE_PGX_DSN not set; skipping pgx integration tests")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	if err := qpgx.Apply(context.Background(), pool); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	queuetest.Run(t, func(t *testing.T) (queue.Service, func()) {
		// Use the test name as a unique namespace so sub-tests don't collide
		// in the shared table.
		svc := qpgx.New(pool, queue.WithNamespace(t.Name()))
		return svc, func() {
			_ = svc.Close(context.Background())
		}
	})
}
