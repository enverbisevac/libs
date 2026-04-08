// Package inmem provides a non-durable, in-process queue.Service.
//
// Jobs live in two per-topic structures: a min-heap of delayed jobs (keyed by
// run_at) and a priority queue of ready jobs (keyed by -priority, then FIFO
// sequence). A timer goroutine moves jobs from delayed to ready as their
// run_at arrives. Workers wait on a sync.Cond and pop from the ready queue.
//
// **Not durable across process restarts.** Use the sqlite backend for
// embedded durability without an external server.
package inmem
