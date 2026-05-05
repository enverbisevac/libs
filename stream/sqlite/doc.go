// Package sqlite provides a durable, embedded stream.Service backed by
// SQLite.
//
// Use it with any database/sql SQLite driver (modernc.org/sqlite,
// mattn/go-sqlite3). The caller is responsible for opening the *sql.DB;
// stream/sqlite does not own the connection lifetime.
//
// SQLite has no FOR UPDATE SKIP LOCKED. Claim queries run inside a
// BEGIN IMMEDIATE transaction; concurrent claimers serialize on the
// database write lock. This is fine for embedded single-process
// workloads. For high throughput across many processes, use the pgx
// backend instead.
//
// The database should be opened with WAL mode and a non-trivial
// busy_timeout for adequate write throughput. Recommended DSN suffix:
// "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
package sqlite
