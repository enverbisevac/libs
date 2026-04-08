// Package pgx provides a durable queue.Service backed by PostgreSQL using
// jackc/pgx/v5.
//
// Workers wake on LISTEN/NOTIFY (channel name = formatted topic) and fall back
// to polling at WithPollInterval. The NOTIFY payload is intentionally empty —
// the channel name alone signals "wake up and re-poll", which sidesteps the
// 8KB Postgres notification payload limit and avoids quoting concerns.
//
// The listener uses one dedicated connection acquired from the pool. If that
// connection drops, notifications are lost until the next subscriber starts;
// the polling fallback ensures jobs are eventually picked up. Auto-reconnect
// is intentionally not implemented — match pubsub/pgx's behavior and document
// the limitation.
package pgx
