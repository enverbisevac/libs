package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "migrate.db") +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var got string
	err := db.QueryRowContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&got)
	if err != nil {
		return false
	}
	return got == name
}

func TestSQL_DefaultTableNameOnEmpty(t *testing.T) {
	db := openSQLite(t)
	if err := SQL(context.Background(), db, "sqlite", ""); err != nil {
		t.Fatalf("SQL: %v", err)
	}
	if !tableExists(t, db, "outbox_messages") {
		t.Error("expected default outbox_messages table to be created when tableName is empty")
	}
}

func TestProcessedSQL_DefaultTableNameOnEmpty(t *testing.T) {
	db := openSQLite(t)
	if err := ProcessedSQL(context.Background(), db, "sqlite", ""); err != nil {
		t.Fatalf("ProcessedSQL: %v", err)
	}
	if !tableExists(t, db, "processed_messages") {
		t.Error("expected default processed_messages table to be created when tableName is empty")
	}
}

func TestProcessedSQL_IsIdempotent(t *testing.T) {
	db := openSQLite(t)
	for range 2 {
		if err := ProcessedSQL(context.Background(), db, "sqlite", ""); err != nil {
			t.Fatalf("ProcessedSQL: %v", err)
		}
	}
}

func TestIsPostgres(t *testing.T) {
	tests := []struct {
		driver string
		want   bool
	}{
		{"postgres", true},
		{"pgx", true},
		{"sqlite3", false},
		{"sqlite", false},
		{"", false},
		{"PGX", false}, // case-sensitive on purpose: matches database/sql driver names verbatim
	}
	for _, tt := range tests {
		if got := isPostgres(tt.driver); got != tt.want {
			t.Errorf("isPostgres(%q) = %v, want %v", tt.driver, got, tt.want)
		}
	}
}
