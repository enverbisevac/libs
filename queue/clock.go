package queue

import "time"

// Clock abstracts time for deterministic testing. The default implementation
// uses the real time package; queuetest provides a controllable FakeClock.
//
// Backends accept a Clock via the (unexported, internal) WithClock option in
// their respective sub-packages — exported only inside this module via
// internal helper functions to keep the public surface small.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer mirrors time.Timer minus the parts queue does not need. Stop returns
// true if the call stops the timer; false if the timer already fired.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// realClock implements Clock using the time package.
type realClock struct{}

// RealClock returns the production Clock implementation.
func RealClock() Clock { return realClock{} }

func (realClock) Now() time.Time { return time.Now() }
func (realClock) NewTimer(d time.Duration) Timer {
	return &realTimer{t: time.NewTimer(d)}
}

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
