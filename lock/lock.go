package lock

import "context"

// Locker represents a distributed lock that can be acquired and released.
type Locker interface {
	// Lock acquires the lock, blocking until it's available or context is cancelled.
	Lock(ctx context.Context) error

	// TryLock attempts to acquire the lock without blocking.
	// Returns true if the lock was acquired, false otherwise.
	TryLock(ctx context.Context) (bool, error)

	// Unlock releases the lock.
	Unlock(ctx context.Context) error
}

// Service provides methods to create distributed locks.
type Service interface {
	// NewLock creates a new lock with the given key.
	NewLock(key string) Locker
}