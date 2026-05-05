// Package inmem provides an in-process stream.Service implementation for
// tests and single-process development.
//
// All state is held in memory — messages, group cursors, and pending
// claims are lost on process exit. The backend honors every public
// contract of the stream package (consumer groups with shared work,
// at-least-once delivery, visibility-timeout reclaim, DLQ routing, trim
// by MaxLen/MaxAge, replay from start positions) so it is interchangeable
// with the durable backends for unit tests.
//
// For production, use the sqlite, pgx, or redis backends.
package inmem
