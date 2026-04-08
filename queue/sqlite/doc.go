// Package sqlite provides a durable, embedded queue.Service backed by SQLite.
//
// Use it with any database/sql SQLite driver (modernc.org/sqlite, mattn/go-sqlite3).
//
// SQLite has no FOR UPDATE SKIP LOCKED. Claim queries run inside a
// BEGIN IMMEDIATE transaction; concurrent claimers serialize on the database
// write lock. This is fine for embedded single-process workloads. For high
// throughput across many processes, use the pgx backend instead.
//
// The database is opened in WAL mode (PRAGMA journal_mode=WAL;
// PRAGMA synchronous=NORMAL) for adequate write throughput. The caller is
// responsible for opening the *sql.DB; queue/sqlite does not own connection
// lifetime.
package sqlite
