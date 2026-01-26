package inmem

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/enverbisevac/libs/lock"
)

// Service implements lock.Service using in-memory mutexes.
type Service struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
}

type lockEntry struct {
	mu      sync.Mutex
	waiters int
}

// New creates a new in-memory lock service.
func New() *Service {
	return &Service{
		locks: make(map[string]*lockEntry),
	}
}

// NewLock creates a new lock with the given key.
func (s *Service) NewLock(key string) lock.Locker {
	return &locker{
		service: s,
		key:     key,
	}
}

func (s *Service) getEntry(key string) *lockEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.locks[key]
	if !ok {
		entry = &lockEntry{}
		s.locks[key] = entry
	}
	entry.waiters++
	return entry
}

func (s *Service) releaseEntry(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.locks[key]
	if !ok {
		return
	}
	entry.waiters--
	if entry.waiters == 0 {
		delete(s.locks, key)
	}
}

type locker struct {
	service *Service
	key     string

	mu     sync.Mutex
	entry  *lockEntry
	locked bool
}

// Lock acquires the lock, blocking until it's available or context is cancelled.
func (l *locker) Lock(ctx context.Context) error {
	l.mu.Lock()
	if l.locked {
		l.mu.Unlock()
		return nil
	}

	entry := l.service.getEntry(l.key)
	l.mu.Unlock()

	// Use atomic flag to track if caller is still interested
	var cancelled atomic.Bool
	done := make(chan struct{})

	go func() {
		entry.mu.Lock()
		// If cancelled before we acquired, release immediately
		if cancelled.Load() {
			entry.mu.Unlock()
			l.service.releaseEntry(l.key)
		}
		close(done)
	}()

	select {
	case <-done:
		// Check if we were cancelled while acquiring
		if cancelled.Load() {
			return ctx.Err()
		}
		l.mu.Lock()
		l.entry = entry
		l.locked = true
		l.mu.Unlock()
		return nil
	case <-ctx.Done():
		cancelled.Store(true)
		// Don't wait for goroutine - it will clean up when it acquires
		return ctx.Err()
	}
}

// TryLock attempts to acquire the lock without blocking.
func (l *locker) TryLock(ctx context.Context) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.locked {
		return true, nil
	}

	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	entry := l.service.getEntry(l.key)

	if entry.mu.TryLock() {
		l.entry = entry
		l.locked = true
		return true, nil
	}

	l.service.releaseEntry(l.key)
	return false, nil
}

// Unlock releases the lock.
func (l *locker) Unlock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.locked {
		return nil
	}

	l.entry.mu.Unlock()
	l.service.releaseEntry(l.key)
	l.entry = nil
	l.locked = false
	return nil
}