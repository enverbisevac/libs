// Package queuetest provides a backend-agnostic conformance test suite for
// queue.Service implementations and a deterministic FakeClock for use in
// backend-specific tests.
//
// Each backend's test file calls queuetest.Run with a factory that produces a
// fresh queue.Service per sub-test. The suite covers FIFO, priority, delay,
// retry, dead letter routing, concurrency, panic recovery, visibility-timeout
// recovery, and graceful shutdown.
package queuetest
