package streambridge_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/enverbisevac/libs/internal/testdb"
	"github.com/enverbisevac/libs/outbox"
	"github.com/enverbisevac/libs/outbox/streambridge"
	"github.com/enverbisevac/libs/stream"
)

// TestNewPostgres_EndToEnd uses STREAM_PGX_DSN if set, otherwise spins up
// a postgres testcontainer. Skips if neither is available.
func TestNewPostgres_EndToEnd(t *testing.T) {
	dsn := testdb.Postgres(t, "STREAM_PGX_DSN")

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	// Conformance-style tests want headroom for listener + workers.
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	b, err := streambridge.NewPostgres(context.Background(), pool, streambridge.PostgresConfig{
		// Per-test table so reruns don't trip on existing data.
		Table:     "outbox_e2e_" + time.Now().Format("20060102150405"),
		RelayOpts: []outbox.Option{outbox.WithPollInterval(20 * time.Millisecond)},
		StreamOpts: []stream.ServiceOption{
			stream.WithNamespace(t.Name()),
		},
	})
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })

	var (
		mu  sync.Mutex
		got [][]byte
	)
	cons, err := b.Subscribe(context.Background(), "events", "g1",
		func(_ context.Context, m *stream.Message) error {
			mu.Lock()
			got = append(got, append([]byte(nil), m.Payload...))
			mu.Unlock()
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = cons.Close() })

	b.Start(context.Background())

	if err := b.Save(context.Background(), nil, outbox.Message{
		Topic:   "events",
		Payload: []byte("hello"),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("handler did not receive the published message")
	}
	if string(got[0]) != "hello" {
		t.Errorf("payload = %q, want hello", string(got[0]))
	}
}

func TestNewPostgres_NilPool(t *testing.T) {
	if _, err := streambridge.NewPostgres(context.Background(), nil, streambridge.PostgresConfig{}); err == nil {
		t.Fatal("expected error for nil pool")
	}
}
