package redis_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/enverbisevac/libs/cache"
	cacheredis "github.com/enverbisevac/libs/cache/redis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time interface check
var (
	_ cache.Cache[string, any]   = (*cacheredis.Cache[string, any])(nil)
	_ cache.Claimer[string, any] = (*cacheredis.Cache[string, any])(nil)
)

func getTestClient(t *testing.T) redis.UniversalClient {
	t.Helper()

	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		addr = os.Getenv("REDIS_ADDR")
	}
	if addr == "" {
		t.Skip("Skipping test: no redis address provided. Set REDIS_TEST_ADDR or REDIS_ADDR environment variable.")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.Ping(ctx).Err(), "redis must be reachable")

	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

// newTestCache returns a Cache whose keys are namespaced with a unique prefix
// so parallel / repeated runs don't collide. Cleanup flushes the namespace.
func newTestCache[V any](t *testing.T) (*cacheredis.Cache[string, V], string) {
	t.Helper()

	client := getTestClient(t)
	prefix := "test:" + time.Now().Format("20060102150405.000000") + ":"

	c := cacheredis.New[string, V](client)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		keys, err := client.Keys(ctx, prefix+"*").Result()
		if err == nil && len(keys) > 0 {
			_ = client.Del(ctx, keys...).Err()
		}
	})

	return c, prefix
}

func TestCache_SetAndGet(t *testing.T) {
	c, p := newTestCache[any](t)

	t.Run("string value", func(t *testing.T) {
		require.NoError(t, c.Set(p+"key1", "value1", time.Hour))

		val, err := c.Get(p + "key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", val)
	})

	t.Run("int value", func(t *testing.T) {
		require.NoError(t, c.Set(p+"key2", 42, time.Hour))

		val, err := c.Get(p + "key2")
		require.NoError(t, err)
		assert.Equal(t, 42, val)
	})

	t.Run("struct value", func(t *testing.T) {
		type testStruct struct {
			Name  string
			Value int
		}

		original := testStruct{Name: "test", Value: 123}
		require.NoError(t, c.Set(p+"key3", original, time.Hour))

		val, err := c.Get(p + "key3")
		require.NoError(t, err)
		assert.Equal(t, original, val)
	})

	t.Run("overwrite existing key", func(t *testing.T) {
		require.NoError(t, c.Set(p+"key4", "first", time.Hour))
		require.NoError(t, c.Set(p+"key4", "second", time.Hour))

		val, err := c.Get(p + "key4")
		require.NoError(t, err)
		assert.Equal(t, "second", val)
	})
}

func TestCache_Get_NotFound(t *testing.T) {
	c, p := newTestCache[string](t)

	val, err := c.Get(p + "nonexistent")
	assert.ErrorIs(t, err, cacheredis.ErrNotFound)
	assert.Empty(t, val)
}

func TestCache_Get_Expired(t *testing.T) {
	c, p := newTestCache[string](t)

	require.NoError(t, c.Set(p+"expiring", "value", 100*time.Millisecond))

	val, err := c.Get(p + "expiring")
	require.NoError(t, err)
	assert.Equal(t, "value", val)

	time.Sleep(200 * time.Millisecond)

	val, err = c.Get(p + "expiring")
	assert.ErrorIs(t, err, cacheredis.ErrNotFound)
	assert.Empty(t, val)
}

func TestCache_Remove(t *testing.T) {
	c, p := newTestCache[string](t)

	t.Run("single key", func(t *testing.T) {
		require.NoError(t, c.Set(p+"remove1", "value", time.Hour))
		require.NoError(t, c.Remove(p+"remove1"))

		_, err := c.Get(p + "remove1")
		assert.ErrorIs(t, err, cacheredis.ErrNotFound)
	})

	t.Run("multiple keys", func(t *testing.T) {
		require.NoError(t, c.Set(p+"remove2", "value2", time.Hour))
		require.NoError(t, c.Set(p+"remove3", "value3", time.Hour))
		require.NoError(t, c.Remove(p+"remove2", p+"remove3"))

		_, err := c.Get(p + "remove2")
		assert.ErrorIs(t, err, cacheredis.ErrNotFound)
		_, err = c.Get(p + "remove3")
		assert.ErrorIs(t, err, cacheredis.ErrNotFound)
	})

	t.Run("nonexistent key", func(t *testing.T) {
		require.NoError(t, c.Remove(p+"nonexistent"))
	})
}

func TestCache_Pop(t *testing.T) {
	c, p := newTestCache[string](t)

	t.Run("existing key", func(t *testing.T) {
		require.NoError(t, c.Set(p+"pop1", "popvalue", time.Hour))

		val, err := c.Pop(p + "pop1")
		require.NoError(t, err)
		assert.Equal(t, "popvalue", val)

		_, err = c.Get(p + "pop1")
		assert.ErrorIs(t, err, cacheredis.ErrNotFound)
	})

	t.Run("nonexistent key", func(t *testing.T) {
		val, err := c.Pop(p + "nonexistent")
		assert.ErrorIs(t, err, cacheredis.ErrNotFound)
		assert.Empty(t, val)
	})
}

func TestCache_Keys(t *testing.T) {
	c, p := newTestCache[string](t)

	require.NoError(t, c.Set(p+"user:1", "alice", time.Hour))
	require.NoError(t, c.Set(p+"user:2", "bob", time.Hour))
	require.NoError(t, c.Set(p+"user:3", "charlie", time.Hour))
	require.NoError(t, c.Set(p+"session:1", "sess1", time.Hour))
	require.NoError(t, c.Set(p+"session:2", "sess2", time.Hour))

	t.Run("prefix match", func(t *testing.T) {
		keys := c.Keys(p + "user:*")
		assert.Len(t, keys, 3)
		assert.Contains(t, keys, p+"user:1")
		assert.Contains(t, keys, p+"user:2")
		assert.Contains(t, keys, p+"user:3")
	})

	t.Run("different prefix", func(t *testing.T) {
		keys := c.Keys(p + "session:*")
		assert.Len(t, keys, 2)
		assert.Contains(t, keys, p+"session:1")
		assert.Contains(t, keys, p+"session:2")
	})

	t.Run("no match", func(t *testing.T) {
		keys := c.Keys(p + "nonexistent:*")
		assert.Len(t, keys, 0)
	})
}

func TestCache_Add(t *testing.T) {
	c, p := newTestCache[any](t)

	t.Run("first caller wins", func(t *testing.T) {
		won, err := c.Add(p+"claim1", "alice", time.Hour)
		require.NoError(t, err)
		assert.True(t, won)

		val, err := c.Get(p + "claim1")
		require.NoError(t, err)
		assert.Equal(t, "alice", val)
	})

	t.Run("second caller loses", func(t *testing.T) {
		won, err := c.Add(p+"claim2", "alice", time.Hour)
		require.NoError(t, err)
		assert.True(t, won)

		won, err = c.Add(p+"claim2", "bob", time.Hour)
		require.NoError(t, err)
		assert.False(t, won)

		val, err := c.Get(p + "claim2")
		require.NoError(t, err)
		assert.Equal(t, "alice", val)
	})

	t.Run("reclaim after expiry", func(t *testing.T) {
		won, err := c.Add(p+"claim3", "alice", 100*time.Millisecond)
		require.NoError(t, err)
		assert.True(t, won)

		time.Sleep(200 * time.Millisecond)

		won, err = c.Add(p+"claim3", "bob", time.Hour)
		require.NoError(t, err)
		assert.True(t, won)

		val, err := c.Get(p + "claim3")
		require.NoError(t, err)
		assert.Equal(t, "bob", val)
	})

	t.Run("concurrent claim — exactly one winner", func(t *testing.T) {
		const contenders = 20
		results := make(chan bool, contenders)
		start := make(chan struct{})

		var wg sync.WaitGroup
		for i := range contenders {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				<-start
				won, err := c.Add(p+"claim_race", id, time.Hour)
				if err != nil {
					t.Errorf("Add failed: %v", err)
				}
				results <- won
			}(i)
		}

		close(start)
		wg.Wait()
		close(results)

		winners := 0
		for won := range results {
			if won {
				winners++
			}
		}
		assert.Equal(t, 1, winners, "expected exactly one winner")
	})
}

func TestCache_LargeValue(t *testing.T) {
	c, p := newTestCache[[]byte](t)

	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	require.NoError(t, c.Set(p+"large", largeData, time.Hour))

	val, err := c.Get(p + "large")
	require.NoError(t, err)
	assert.Equal(t, largeData, val)
}

func TestCache_NilValue(t *testing.T) {
	c, p := newTestCache[any](t)

	require.NoError(t, c.Set(p+"nilkey", nil, time.Hour))

	val, err := c.Get(p + "nilkey")
	require.NoError(t, err)
	assert.Nil(t, val)
}
