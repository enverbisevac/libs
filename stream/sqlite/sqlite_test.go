package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

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
