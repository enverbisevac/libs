package queuetest

import (
	"sort"
	"sync"
	"time"

	"github.com/enverbisevac/libs/queue"
)

// FakeClock is a deterministic Clock controlled by tests.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock returns a FakeClock anchored at t (zero time = time.Unix(0,0)).
func NewFakeClock(t time.Time) *FakeClock {
	if t.IsZero() {
		t = time.Unix(0, 0)
	}
	return &FakeClock{now: t}
}

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d, firing any timers whose deadline has
// elapsed.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	due := c.dueLocked()
	c.mu.Unlock()
	for _, t := range due {
		t.fire(c.now)
	}
}

func (c *FakeClock) dueLocked() []*fakeTimer {
	var due []*fakeTimer
	remaining := c.timers[:0]
	for _, t := range c.timers {
		if !t.fireAt.After(c.now) {
			due = append(due, t)
			continue
		}
		remaining = append(remaining, t)
	}
	c.timers = remaining
	sort.SliceStable(due, func(i, j int) bool {
		return due[i].fireAt.Before(due[j].fireAt)
	})
	return due
}

// NewTimer creates a queue.Timer that fires after d (relative to the current
// fake time).
func (c *FakeClock) NewTimer(d time.Duration) queue.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{
		clock:  c,
		fireAt: c.now.Add(d),
		ch:     make(chan time.Time, 1),
	}
	c.timers = append(c.timers, t)
	return t
}

type fakeTimer struct {
	clock  *FakeClock
	fireAt time.Time
	ch     chan time.Time
	fired  bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	for i, x := range t.clock.timers {
		if x == t {
			t.clock.timers = append(t.clock.timers[:i], t.clock.timers[i+1:]...)
			return true
		}
	}
	return false
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	stopped := t.Stop()
	t.clock.mu.Lock()
	t.fireAt = t.clock.now.Add(d)
	t.fired = false
	t.clock.timers = append(t.clock.timers, t)
	t.clock.mu.Unlock()
	return stopped
}

func (t *fakeTimer) fire(now time.Time) {
	if t.fired {
		return
	}
	t.fired = true
	select {
	case t.ch <- now:
	default:
	}
}
