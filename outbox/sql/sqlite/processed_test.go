package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "processed.db") +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := MigrateProcessed(context.Background(), db, ""); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func TestNewProcessedDefaultTable(t *testing.T) {
	if got := NewProcessed("").table; got != DefaultProcessedTable {
		t.Errorf("table = %q, want %q", got, DefaultProcessedTable)
	}
	if got := NewProcessed("custom").table; got != "custom" {
		t.Errorf("table = %q, want %q", got, "custom")
	}
}

func TestCreateProcessedTableSQL(t *testing.T) {
	s := CreateProcessedTableSQL("processed_messages")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS processed_messages",
		"group_name   TEXT NOT NULL",
		"message_id   TEXT NOT NULL",
		"processed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
		"metadata     TEXT",
		"PRIMARY KEY (group_name, message_id)",
		"idx_processed_messages_processed_at",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("missing fragment %q in DDL", want)
		}
	}
}

func TestMigrateProcessedIsIdempotent(t *testing.T) {
	db := openDB(t)
	// openDB already migrated once; a second call must succeed.
	if err := MigrateProcessed(context.Background(), db, ""); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func TestProcessedStore_RoundTrip(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	s := NewProcessed("")

	got, err := s.IsProcessed(ctx, db, "g1", "m1")
	if err != nil {
		t.Fatalf("IsProcessed before mark: %v", err)
	}
	if got {
		t.Fatal("IsProcessed returned true on empty table")
	}

	isNew, err := s.MarkProcessed(ctx, db, "g1", "m1", nil)
	if err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	if !isNew {
		t.Fatal("first MarkProcessed returned isNew=false")
	}

	got, err = s.IsProcessed(ctx, db, "g1", "m1")
	if err != nil {
		t.Fatalf("IsProcessed after mark: %v", err)
	}
	if !got {
		t.Fatal("IsProcessed returned false after MarkProcessed")
	}

	// Second mark on the same (group, msgID) collapses via ON CONFLICT.
	isNew, err = s.MarkProcessed(ctx, db, "g1", "m1", nil)
	if err != nil {
		t.Fatalf("MarkProcessed (second call): %v", err)
	}
	if isNew {
		t.Error("second MarkProcessed returned isNew=true; expected false")
	}

	// Different group, same message id is independent.
	isNew, err = s.MarkProcessed(ctx, db, "g2", "m1", nil)
	if err != nil {
		t.Fatalf("MarkProcessed (different group): %v", err)
	}
	if !isNew {
		t.Error("MarkProcessed for different group returned isNew=false")
	}
}

func TestProcessedStore_MarkRollbackReleasesMark(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	s := NewProcessed("")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := s.MarkProcessed(ctx, tx, "g", "m1", nil); err != nil {
		t.Fatalf("MarkProcessed in tx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	got, err := s.IsProcessed(ctx, db, "g", "m1")
	if err != nil {
		t.Fatalf("IsProcessed after rollback: %v", err)
	}
	if got {
		t.Error("rolled-back mark visible to IsProcessed; transactional semantics broken")
	}
}

func TestProcessedStore_MetadataValid(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	s := NewProcessed("")

	meta := []byte(`{"printer":"ZD220","ok":true}`)
	if _, err := s.MarkProcessed(ctx, db, "g", "m1", meta); err != nil {
		t.Fatalf("MarkProcessed with valid metadata: %v", err)
	}

	var stored string
	err := db.QueryRowContext(ctx,
		`SELECT metadata FROM processed_messages WHERE group_name=? AND message_id=?`,
		"g", "m1",
	).Scan(&stored)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if stored != string(meta) {
		t.Errorf("stored metadata = %q, want %q", stored, string(meta))
	}
}

func TestProcessedStore_MetadataInvalidRejected(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	s := NewProcessed("")

	_, err := s.MarkProcessed(ctx, db, "g", "m1", []byte(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON metadata")
	}
	if !strings.Contains(err.Error(), "not valid JSON") {
		t.Errorf("error = %v, want substring 'not valid JSON'", err)
	}

	// And nothing was inserted.
	got, _ := s.IsProcessed(ctx, db, "g", "m1")
	if got {
		t.Error("invalid-metadata insert should not have created a row")
	}
}

func TestProcessedStore_PurgeProcessedBefore(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	s := NewProcessed("")

	if _, err := s.MarkProcessed(ctx, db, "g", "old", nil); err != nil {
		t.Fatalf("mark old: %v", err)
	}
	if _, err := s.MarkProcessed(ctx, db, "g", "new", nil); err != nil {
		t.Fatalf("mark new: %v", err)
	}

	// Backdate the "old" row using the same shape CURRENT_TIMESTAMP
	// produces ("YYYY-MM-DD HH:MM:SS"), which is what
	// PurgeProcessedBefore compares against.
	past := time.Now().Add(-2 * time.Hour).UTC().Format("2006-01-02 15:04:05")
	if _, err := db.ExecContext(ctx,
		`UPDATE processed_messages SET processed_at = ? WHERE message_id = ?`,
		past, "old",
	); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	cutoff := time.Now().Add(-time.Hour)
	n, err := s.PurgeProcessedBefore(ctx, db, cutoff)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if n != 1 {
		t.Errorf("purged %d rows, want 1", n)
	}

	if got, _ := s.IsProcessed(ctx, db, "g", "old"); got {
		t.Error("'old' row still present after purge")
	}
	if got, _ := s.IsProcessed(ctx, db, "g", "new"); !got {
		t.Error("'new' row was purged but should not have been")
	}
}

func TestProcessedStore_ConcurrentMarkOnlyOneIsNew(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	s := NewProcessed("")

	const workers = 16
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		newCount  int
		seenError error
	)
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			isNew, err := s.MarkProcessed(ctx, db, "g", "shared", nil)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				seenError = err
				return
			}
			if isNew {
				newCount++
			}
		}()
	}
	wg.Wait()

	if seenError != nil {
		t.Fatalf("unexpected error: %v", seenError)
	}
	if newCount != 1 {
		t.Errorf("isNew count = %d, want exactly 1 (ON CONFLICT must collapse the rest)", newCount)
	}
}

func TestProcessedStore_RejectsBadDBHandle(t *testing.T) {
	s := NewProcessed("")
	type wrong struct{}

	if _, err := s.IsProcessed(context.Background(), wrong{}, "g", "m"); err == nil {
		t.Error("IsProcessed: expected error for unsupported db type")
	}
	if _, err := s.MarkProcessed(context.Background(), wrong{}, "g", "m", nil); err == nil {
		t.Error("MarkProcessed: expected error for unsupported db type")
	}
	if _, err := s.PurgeProcessedBefore(context.Background(), wrong{}, time.Now()); err == nil {
		t.Error("PurgeProcessedBefore: expected error for unsupported db type")
	}
}
