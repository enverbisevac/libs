package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/enverbisevac/libs/stream"
	ssqlite "github.com/enverbisevac/libs/stream/sqlite"
	"github.com/enverbisevac/libs/stream/streamtest"
)

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "stream.db") + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := ssqlite.Apply(context.Background(), db); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return db
}

func TestConformance(t *testing.T) {
	streamtest.Run(t, func(t *testing.T) (stream.Service, func()) {
		// Each sub-test gets its own DB so durable state cannot bleed.
		db := openDB(t)
		svc := ssqlite.New(db, stream.WithNamespace(t.Name()))
		return svc, func() { _ = svc.Close(context.Background()) }
	})
}

// TestCloseHonorsContext verifies that Service.Close bounds an in-flight
// handler drain by the caller's ctx, not the (much larger) per-consumer
// ShutdownTimeout. Before this fix, ctx only gated the gap between consumers,
// so a single stuck handler made Close block the full ShutdownTimeout.
func TestCloseHonorsContext(t *testing.T) {
	db := openDB(t)
	svc := ssqlite.New(db, stream.WithNamespace(t.Name()))

	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release) // let the wedged handler unwind after the assertions

	// Handler blocks until released and ignores ctx — models a wedged
	// in-flight print (a non-cancellable CUPS submit).
	h := func(ctx context.Context, msg *stream.Message) error {
		close(started)
		<-release
		return nil
	}

	// 30s ShutdownTimeout: the ONLY thing that may bound Close is the ctx.
	if _, err := svc.Subscribe(context.Background(), "evt", "g1", h,
		stream.WithShutdownTimeout(30*time.Second)); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := svc.Publish(context.Background(), "evt", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never reached in-flight state")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := svc.Close(ctx)
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Fatalf("Close blocked %v — ctx deadline not honored (ShutdownTimeout was 30s)", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close should report the ctx deadline, got %v", err)
	}
	t.Logf("Close returned in %v with %v (ShutdownTimeout was 30s) ✓", elapsed.Round(time.Millisecond), err)
}
