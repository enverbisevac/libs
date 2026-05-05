package streambridge_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/enverbisevac/libs/outbox"
	"github.com/enverbisevac/libs/outbox/streambridge"
	"github.com/enverbisevac/libs/stream"
)

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "bridge.db") +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestNewSQLite_EndToEnd verifies the full path: Save into the outbox in
// a transaction, Start the relay, and observe the message arrive via a
// stream Subscribe handler — all wired with one constructor.
func TestNewSQLite_EndToEnd(t *testing.T) {
	db := openSQLite(t)

	b, err := streambridge.NewSQLite(context.Background(), db, streambridge.SQLiteConfig{
		RelayOpts: []outbox.Option{outbox.WithPollInterval(20 * time.Millisecond)},
	})
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
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

	// Save through a real *sql.Tx so we exercise the transactional path.
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := b.Save(context.Background(), tx, outbox.Message{
		Topic:   "events",
		Payload: []byte("hello"),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
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

func TestNewSQLite_NilDB(t *testing.T) {
	if _, err := streambridge.NewSQLite(context.Background(), nil, streambridge.SQLiteConfig{}); err == nil {
		t.Fatal("expected error for nil db")
	}
}

func TestNewSQLite_CustomTable(t *testing.T) {
	db := openSQLite(t)

	b, err := streambridge.NewSQLite(context.Background(), db, streambridge.SQLiteConfig{
		Table: "my_events_outbox",
	})
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })

	// The custom table must exist.
	var name string
	err = db.QueryRowContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type='table' AND name=?`,
		"my_events_outbox",
	).Scan(&name)
	if err != nil {
		t.Fatalf("custom table not created: %v", err)
	}
	if name != "my_events_outbox" {
		t.Errorf("table = %q, want my_events_outbox", name)
	}
}
