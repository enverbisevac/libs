package pgx_test

import (
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"testing"
	"time"

	"github.com/enverbisevac/libs/cache"
	"github.com/enverbisevac/libs/cache/pgx"
	"github.com/enverbisevac/libs/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testStruct is the concrete type used by the "struct value" subtest. It
// must live at package scope and be registered with gob so decoding via
// the V=any path can reconstruct it. Local types declared inside a subtest
// have no stable name from gob's perspective and fail with
// "type not registered for interface".
type testStruct struct {
	Name  string
	Value int
}

func init() {
	gob.Register(testStruct{})
}

func getTestDSN(t *testing.T) string {
	t.Helper()
	return testdb.Postgres(t, "PGX_TEST_DATABASE_URL")
}

func getTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbURL := getTestDSN(t)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)

	t.Cleanup(func() {
		pool.Close()
	})

	return pool
}

func newTestCache[V any](t *testing.T, opts ...pgx.Option[string, V]) *pgx.Cache[string, V] {
	t.Helper()

	pool := getTestPool(t)
	ctx := context.Background()

	// Unique table name per test invocation. UnixNano avoids the dot in
	// fractional-second formats — Postgres parses unquoted "x.y" as
	// schema.table and chokes when the right side starts with a digit.
	tableName := fmt.Sprintf("test_cache_%d", time.Now().UnixNano())
	opts = append([]pgx.Option[string, V]{pgx.WithTableName[string, V](tableName)}, opts...)

	c, err := pgx.New(ctx, pool, opts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
		_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
	})

	return c
}

// Compile-time interface check
var _ cache.Cache[string, any] = (*pgx.Cache[string, any])(nil)

func TestCache_SetAndGet(t *testing.T) {
	c := newTestCache[any](t)
	ctx := context.Background()
	_ = ctx

	t.Run("string value", func(t *testing.T) {
		err := c.Set("key1", "value1", time.Hour)
		require.NoError(t, err)

		val, err := c.Get("key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", val)
	})

	t.Run("int value", func(t *testing.T) {
		err := c.Set("key2", 42, time.Hour)
		require.NoError(t, err)

		val, err := c.Get("key2")
		require.NoError(t, err)
		assert.Equal(t, 42, val)
	})

	t.Run("struct value", func(t *testing.T) {
		original := testStruct{Name: "test", Value: 123}
		err := c.Set("key3", original, time.Hour)
		require.NoError(t, err)

		val, err := c.Get("key3")
		require.NoError(t, err)
		assert.Equal(t, original, val)
	})

	t.Run("overwrite existing key", func(t *testing.T) {
		err := c.Set("key4", "first", time.Hour)
		require.NoError(t, err)

		err = c.Set("key4", "second", time.Hour)
		require.NoError(t, err)

		val, err := c.Get("key4")
		require.NoError(t, err)
		assert.Equal(t, "second", val)
	})
}

func TestCache_Get_NotFound(t *testing.T) {
	c := newTestCache[string](t)

	val, err := c.Get("nonexistent")
	assert.ErrorIs(t, err, pgx.ErrNotFound)
	assert.Empty(t, val)
}

func TestCache_Get_Expired(t *testing.T) {
	c := newTestCache[string](t)

	// Set with very short TTL
	err := c.Set("expiring", "value", 50*time.Millisecond)
	require.NoError(t, err)

	// Should exist initially
	val, err := c.Get("expiring")
	require.NoError(t, err)
	assert.Equal(t, "value", val)

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Should be expired now
	val, err = c.Get("expiring")
	assert.ErrorIs(t, err, pgx.ErrNotFound)
	assert.Empty(t, val)
}

func TestCache_Remove(t *testing.T) {
	c := newTestCache[string](t)

	t.Run("single key", func(t *testing.T) {
		err := c.Set("remove1", "value", time.Hour)
		require.NoError(t, err)

		err = c.Remove("remove1")
		require.NoError(t, err)

		_, err = c.Get("remove1")
		assert.ErrorIs(t, err, pgx.ErrNotFound)
	})

	t.Run("multiple keys", func(t *testing.T) {
		err := c.Set("remove2", "value2", time.Hour)
		require.NoError(t, err)
		err = c.Set("remove3", "value3", time.Hour)
		require.NoError(t, err)

		err = c.Remove("remove2", "remove3")
		require.NoError(t, err)

		_, err = c.Get("remove2")
		assert.ErrorIs(t, err, pgx.ErrNotFound)
		_, err = c.Get("remove3")
		assert.ErrorIs(t, err, pgx.ErrNotFound)
	})

	t.Run("nonexistent key", func(t *testing.T) {
		err := c.Remove("nonexistent")
		require.NoError(t, err) // Should not error
	})

	t.Run("empty keys", func(t *testing.T) {
		err := c.Remove()
		require.NoError(t, err) // Should not error
	})
}

func TestCache_Pop(t *testing.T) {
	c := newTestCache[string](t)

	t.Run("existing key", func(t *testing.T) {
		err := c.Set("pop1", "popvalue", time.Hour)
		require.NoError(t, err)

		val, err := c.Pop("pop1")
		require.NoError(t, err)
		assert.Equal(t, "popvalue", val)

		// Should be gone after pop
		_, err = c.Get("pop1")
		assert.ErrorIs(t, err, pgx.ErrNotFound)
	})

	t.Run("nonexistent key", func(t *testing.T) {
		val, err := c.Pop("nonexistent")
		assert.ErrorIs(t, err, pgx.ErrNotFound)
		assert.Empty(t, val)
	})

	t.Run("expired key", func(t *testing.T) {
		err := c.Set("pop_expired", "value", 50*time.Millisecond)
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		val, err := c.Pop("pop_expired")
		assert.ErrorIs(t, err, pgx.ErrNotFound)
		assert.Empty(t, val)
	})
}

func TestCache_Keys(t *testing.T) {
	c := newTestCache[string](t)

	// Set up test data
	require.NoError(t, c.Set("user:1", "alice", time.Hour))
	require.NoError(t, c.Set("user:2", "bob", time.Hour))
	require.NoError(t, c.Set("user:3", "charlie", time.Hour))
	require.NoError(t, c.Set("session:1", "sess1", time.Hour))
	require.NoError(t, c.Set("session:2", "sess2", time.Hour))

	t.Run("prefix match", func(t *testing.T) {
		keys := c.Keys("user:")
		assert.Len(t, keys, 3)
		assert.Contains(t, keys, "user:1")
		assert.Contains(t, keys, "user:2")
		assert.Contains(t, keys, "user:3")
	})

	t.Run("different prefix", func(t *testing.T) {
		keys := c.Keys("session:")
		assert.Len(t, keys, 2)
		assert.Contains(t, keys, "session:1")
		assert.Contains(t, keys, "session:2")
	})

	t.Run("no match", func(t *testing.T) {
		keys := c.Keys("nonexistent:")
		assert.Len(t, keys, 0)
	})

	t.Run("empty prefix matches all", func(t *testing.T) {
		keys := c.Keys("")
		assert.Len(t, keys, 5)
	})

	t.Run("excludes expired keys", func(t *testing.T) {
		require.NoError(t, c.Set("temp:1", "value", 50*time.Millisecond))

		// Should exist initially
		keys := c.Keys("temp:")
		assert.Len(t, keys, 1)

		time.Sleep(100 * time.Millisecond)

		// Should be excluded after expiration
		keys = c.Keys("temp:")
		assert.Len(t, keys, 0)
	})
}

func TestCache_Close(t *testing.T) {
	c := newTestCache[any](t)

	// Should not panic or error
	err := c.Close()
	require.NoError(t, err)

	// Double close should be safe
	err = c.Close()
	require.NoError(t, err)
}

func TestCache_WithTableName(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()

	customTable := "custom_cache_table_test"

	c, err := pgx.New(ctx, pool, pgx.WithTableName[string, string](customTable))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
		_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+customTable)
	})

	// Verify table was created with a custom name
	var exists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_name = $1
		)
	`, customTable).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists)

	// Should work normally
	err = c.Set("test", "value", time.Hour)
	require.NoError(t, err)

	val, err := c.Get("test")
	require.NoError(t, err)
	assert.Equal(t, "value", val)
}

func TestCache_Add(t *testing.T) {
	c := newTestCache[any](t)

	t.Run("first caller wins", func(t *testing.T) {
		won, err := c.Add("claim1", "alice", time.Hour)
		require.NoError(t, err)
		assert.True(t, won)

		val, err := c.Get("claim1")
		require.NoError(t, err)
		assert.Equal(t, "alice", val)
	})

	t.Run("second caller loses", func(t *testing.T) {
		won, err := c.Add("claim2", "alice", time.Hour)
		require.NoError(t, err)
		assert.True(t, won)

		won, err = c.Add("claim2", "bob", time.Hour)
		require.NoError(t, err)
		assert.False(t, won)

		// Value stays with the winner.
		val, err := c.Get("claim2")
		require.NoError(t, err)
		assert.Equal(t, "alice", val)
	})

	t.Run("reclaim after expiry", func(t *testing.T) {
		won, err := c.Add("claim3", "alice", 50*time.Millisecond)
		require.NoError(t, err)
		assert.True(t, won)

		time.Sleep(100 * time.Millisecond)

		won, err = c.Add("claim3", "bob", time.Hour)
		require.NoError(t, err)
		assert.True(t, won)

		val, err := c.Get("claim3")
		require.NoError(t, err)
		assert.Equal(t, "bob", val)
	})

	t.Run("concurrent claim — exactly one winner", func(t *testing.T) {
		const contenders = 20
		results := make(chan bool, contenders)
		start := make(chan struct{})

		for i := range contenders {
			go func(id int) {
				<-start
				won, err := c.Add("claim_race", id, time.Hour)
				if err != nil {
					t.Errorf("Add failed: %v", err)
				}
				results <- won
			}(i)
		}

		close(start)

		winners := 0
		for range contenders {
			if <-results {
				winners++
			}
		}
		assert.Equal(t, 1, winners, "expected exactly one winner, got %d", winners)
	})
}

func TestCache_ConcurrentAccess(t *testing.T) {
	c := newTestCache[int](t)

	const goroutines = 10
	const operations = 100

	done := make(chan bool, goroutines)

	for i := range goroutines {
		go func(id int) {
			for j := range operations {
				key := "concurrent:" + string(rune('A'+id)) + ":" + string(rune('0'+j%10))

				_ = c.Set(key, j, time.Hour)
				_, _ = c.Get(key)
				_ = c.Keys("concurrent:")
			}
			done <- true
		}(i)
	}

	for range goroutines {
		<-done
	}
}

func TestCache_LargeValue(t *testing.T) {
	c := newTestCache[[]byte](t)

	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	err := c.Set("large", largeData, time.Hour)
	require.NoError(t, err)

	val, err := c.Get("large")
	require.NoError(t, err)
	assert.Equal(t, largeData, val)
}

func TestCache_SpecialCharactersInKey(t *testing.T) {
	c := newTestCache[string](t)

	testCases := []string{
		"key with spaces",
		"key:with:colons",
		"key/with/slashes",
		"key.with.dots",
		"key-with-dashes",
		"key_with_underscores",
		"key@with#special$chars%",
		"unicode:日本語",
	}

	for _, key := range testCases {
		t.Run(key, func(t *testing.T) {
			err := c.Set(key, "value", time.Hour)
			require.NoError(t, err)

			val, err := c.Get(key)
			require.NoError(t, err)
			assert.Equal(t, "value", val)
		})
	}
}

func TestCache_NilValue(t *testing.T) {
	c := newTestCache[any](t)

	err := c.Set("nilkey", nil, time.Hour)
	require.NoError(t, err)

	val, err := c.Get("nilkey")
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestCache_ZeroTTL(t *testing.T) {
	c := newTestCache[string](t)

	// Zero TTL means immediately expired
	err := c.Set("zero_ttl", "value", 0)
	require.NoError(t, err)

	// Should be expired immediately
	_, err = c.Get("zero_ttl")
	assert.ErrorIs(t, err, pgx.ErrNotFound)
}

func TestCache_StdLib(t *testing.T) {
	dbURL := getTestDSN(t)

	ctx := context.Background()

	// Open stdlib connection using pgx stdlib adapter
	db, err := sql.Open("pgx", dbURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	tableName := "test_cache_stdlib_" + time.Now().Format("20060102150405")
	c, err := pgx.NewStdLib(ctx, db, pgx.WithTableName[string, string](tableName))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = c.Close()
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName)
	})

	// Test basic operations
	err = c.Set("stdlib_key", "stdlib_value", time.Hour)
	require.NoError(t, err)

	val, err := c.Get("stdlib_key")
	require.NoError(t, err)
	assert.Equal(t, "stdlib_value", val)

	err = c.Remove("stdlib_key")
	require.NoError(t, err)

	_, err = c.Get("stdlib_key")
	assert.ErrorIs(t, err, pgx.ErrNotFound)
}
