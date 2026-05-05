// Package pgx provides a distributed stream.Service backed by Postgres.
//
// The caller owns the *pgxpool.Pool; this package never opens or closes
// the connection and never reads DATABASE_URL.
//
// Claim uses FOR UPDATE SKIP LOCKED so multiple workers (in the same
// process or across processes) can compete safely on the same consumer
// group without double-delivery. LISTEN/NOTIFY wakes subscribers
// immediately on Publish; PollInterval is the fallback wake-up cadence
// for missed notifications and stuck-pending reclaim.
package pgx
