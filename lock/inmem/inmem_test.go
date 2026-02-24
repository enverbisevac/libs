package inmem

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	svc := New()
	lock := svc.NewLock("test-key")

	ctx := context.Background()

	if err := lock.Lock(ctx); err != nil {
		t.Fatalf("Lock() error = %v", err)
	}

	if err := lock.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}
}

func TestLockReentrant(t *testing.T) {
	svc := New()
	lock := svc.NewLock("test-key")

	ctx := context.Background()

	if err := lock.Lock(ctx); err != nil {
		t.Fatalf("Lock() error = %v", err)
	}

	// Second lock on same locker should return immediately
	if err := lock.Lock(ctx); err != nil {
		t.Fatalf("Lock() second call error = %v", err)
	}

	if err := lock.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}
}

func TestTryLock(t *testing.T) {
	svc := New()
	lock := svc.NewLock("test-key")

	ctx := context.Background()

	acquired, err := lock.TryLock(ctx)
	if err != nil {
		t.Fatalf("TryLock() error = %v", err)
	}
	if !acquired {
		t.Fatal("TryLock() should acquire lock")
	}

	if err := lock.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}
}

func TestTryLockFails(t *testing.T) {
	svc := New()
	lock1 := svc.NewLock("test-key")
	lock2 := svc.NewLock("test-key")

	ctx := context.Background()

	acquired, err := lock1.TryLock(ctx)
	if err != nil {
		t.Fatalf("TryLock() error = %v", err)
	}
	if !acquired {
		t.Fatal("TryLock() should acquire lock")
	}

	// Second locker should fail to acquire
	acquired, err = lock2.TryLock(ctx)
	if err != nil {
		t.Fatalf("TryLock() error = %v", err)
	}
	if acquired {
		t.Fatal("TryLock() should fail when lock is held")
	}

	if err := lock1.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}
}

func TestLockContextCancelled(t *testing.T) {
	svc := New()
	lock1 := svc.NewLock("test-key")
	lock2 := svc.NewLock("test-key")

	ctx := context.Background()

	if err := lock1.Lock(ctx); err != nil {
		t.Fatalf("Lock() error = %v", err)
	}

	// Try to lock with cancelled context
	ctx2, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := lock2.Lock(ctx2)
	if err != context.DeadlineExceeded {
		t.Fatalf("Lock() error = %v, want %v", err, context.DeadlineExceeded)
	}

	if err := lock1.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}
}

func TestTryLockContextCancelled(t *testing.T) {
	svc := New()
	lock := svc.NewLock("test-key")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := lock.TryLock(ctx)
	if err != context.Canceled {
		t.Fatalf("TryLock() error = %v, want %v", err, context.Canceled)
	}
}

func TestUnlockIdempotent(t *testing.T) {
	svc := New()
	lock := svc.NewLock("test-key")

	ctx := context.Background()

	// Unlock without lock should be no-op
	if err := lock.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}

	if err := lock.Lock(ctx); err != nil {
		t.Fatalf("Lock() error = %v", err)
	}

	if err := lock.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}

	// Double unlock should be no-op
	if err := lock.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() second call error = %v", err)
	}
}

func TestDifferentKeys(t *testing.T) {
	svc := New()
	lock1 := svc.NewLock("key1")
	lock2 := svc.NewLock("key2")

	ctx := context.Background()

	acquired1, err := lock1.TryLock(ctx)
	if err != nil {
		t.Fatalf("TryLock() error = %v", err)
	}
	if !acquired1 {
		t.Fatal("TryLock() should acquire lock for key1")
	}

	// Different key should acquire independently
	acquired2, err := lock2.TryLock(ctx)
	if err != nil {
		t.Fatalf("TryLock() error = %v", err)
	}
	if !acquired2 {
		t.Fatal("TryLock() should acquire lock for key2")
	}

	if err := lock1.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}
	if err := lock2.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	svc := New()
	ctx := context.Background()

	var counter int64
	var wg sync.WaitGroup

	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			lock := svc.NewLock("counter")
			if err := lock.Lock(ctx); err != nil {
				t.Errorf("Lock() error = %v", err)
				return
			}

			// Critical section
			val := atomic.LoadInt64(&counter)
			time.Sleep(time.Microsecond)
			atomic.StoreInt64(&counter, val+1)

			if err := lock.Unlock(ctx); err != nil {
				t.Errorf("Unlock() error = %v", err)
			}
		}()
	}

	wg.Wait()

	if counter != 100 {
		t.Fatalf("counter = %d, want 100", counter)
	}
}

func TestLockEntryCleanup(t *testing.T) {
	svc := New()
	lock := svc.NewLock("test-key")

	ctx := context.Background()

	if err := lock.Lock(ctx); err != nil {
		t.Fatalf("Lock() error = %v", err)
	}

	svc.mu.Lock()
	if len(svc.locks) != 1 {
		t.Fatalf("locks map len = %d, want 1", len(svc.locks))
	}
	svc.mu.Unlock()

	if err := lock.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}

	svc.mu.Lock()
	if len(svc.locks) != 0 {
		t.Fatalf("locks map len = %d, want 0 (should be cleaned up)", len(svc.locks))
	}
	svc.mu.Unlock()
}
