package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/enverbisevac/libs/queue"
	"github.com/enverbisevac/libs/queue/queuetest"
	qsqlite "github.com/enverbisevac/libs/queue/sqlite"
)

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "queue.db") + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := qsqlite.Apply(context.Background(), db); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return db
}

func TestConformance(t *testing.T) {
	queuetest.Run(t, func(t *testing.T) (queue.Service, func()) {
		db := openDB(t)
		svc := qsqlite.New(db, queue.WithNamespace(t.Name()))
		return svc, func() { _ = svc.Close(context.Background()) }
	})
}
