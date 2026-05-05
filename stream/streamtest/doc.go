// Package streamtest provides a backend-agnostic conformance test suite
// for stream.Service implementations.
//
// Each backend's test file calls streamtest.Run with a factory that
// produces a fresh stream.Service per sub-test. The suite covers the full
// surface of the stream package: round-trip, consumer-group fan-in,
// fan-out across groups, replay/start-position, visibility-timeout
// reclaim, MaxRetries + DLQ routing, ErrSkipRetry, headers, MaxLen /
// MaxAge trim, and concurrency under the race detector.
package streamtest
