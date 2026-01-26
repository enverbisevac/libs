package pgx

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping pgx lock tests")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	return pool
}

func TestLock(t *testing.T) {
	pool := getTestPool(t)
	svc := New(pool)
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
	pool := getTestPool(t)
	svc := New(pool)
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
	pool := getTestPool(t)
	svc := New(pool)
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
	pool := getTestPool(t)
	svc := New(pool)
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
	pool := getTestPool(t)
	svc := New(pool)
	lock1 := svc.NewLock("test-key-cancel")
	lock2 := svc.NewLock("test-key-cancel")

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

func TestUnlockIdempotent(t *testing.T) {
	pool := getTestPool(t)
	svc := New(pool)
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
	pool := getTestPool(t)
	svc := New(pool)
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

func TestNamespace(t *testing.T) {
	pool := getTestPool(t)
	svc1 := New(pool, WithNamespace(1))
	svc2 := New(pool, WithNamespace(2))

	lock1 := svc1.NewLock("same-key")
	lock2 := svc2.NewLock("same-key")

	ctx := context.Background()

	acquired1, err := lock1.TryLock(ctx)
	if err != nil {
		t.Fatalf("TryLock() error = %v", err)
	}
	if !acquired1 {
		t.Fatal("TryLock() should acquire lock in namespace 1")
	}

	// Same key but different namespace should acquire independently
	acquired2, err := lock2.TryLock(ctx)
	if err != nil {
		t.Fatalf("TryLock() error = %v", err)
	}
	if !acquired2 {
		t.Fatal("TryLock() should acquire lock in namespace 2")
	}

	if err := lock1.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}
	if err := lock2.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	pool := getTestPool(t)
	svc := New(pool)
	ctx := context.Background()

	var counter int64
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
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
			time.Sleep(time.Millisecond)
			atomic.StoreInt64(&counter, val+1)

			if err := lock.Unlock(ctx); err != nil {
				t.Errorf("Unlock() error = %v", err)
			}
		}()
	}

	wg.Wait()

	if counter != 20 {
		t.Fatalf("counter = %d, want 20", counter)
	}
}

func TestLockAfterUnlock(t *testing.T) {
	pool := getTestPool(t)
	svc := New(pool)
	lock1 := svc.NewLock("test-key")
	lock2 := svc.NewLock("test-key")

	ctx := context.Background()

	// First locker acquires and releases
	if err := lock1.Lock(ctx); err != nil {
		t.Fatalf("Lock() error = %v", err)
	}
	if err := lock1.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}

	// Second locker should be able to acquire
	acquired, err := lock2.TryLock(ctx)
	if err != nil {
		t.Fatalf("TryLock() error = %v", err)
	}
	if !acquired {
		t.Fatal("TryLock() should acquire lock after previous holder released")
	}

	if err := lock2.Unlock(ctx); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}
}

func TestHashKey(t *testing.T) {
	// Same key should produce same hash
	hash1 := hashKey("test-key")
	hash2 := hashKey("test-key")
	if hash1 != hash2 {
		t.Fatalf("hashKey() not deterministic: %d != %d", hash1, hash2)
	}

	// Different keys should produce different hashes (usually)
	hash3 := hashKey("other-key")
	if hash1 == hash3 {
		t.Log("Warning: hash collision detected (unlikely but possible)")
	}
}