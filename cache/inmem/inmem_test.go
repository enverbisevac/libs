package inmem_test

import (
	"sync"
	"testing"
	"time"

	"github.com/enverbisevac/libs/cache"
	"github.com/enverbisevac/libs/cache/inmem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time interface check
var (
	_ cache.Cache[string, any]   = (*inmem.Cache[string, any])(nil)
	_ cache.Claimer[string, any] = (*inmem.Cache[string, any])(nil)
)

func TestCache_SetAndGet(t *testing.T) {
	c := inmem.New[string, any]()

	t.Run("string value", func(t *testing.T) {
		require.NoError(t, c.Set("key1", "value1", time.Hour))

		val, err := c.Get("key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", val)
	})

	t.Run("int value", func(t *testing.T) {
		require.NoError(t, c.Set("key2", 42, time.Hour))

		val, err := c.Get("key2")
		require.NoError(t, err)
		assert.Equal(t, 42, val)
	})

	t.Run("struct value", func(t *testing.T) {
		type testStruct struct {
			Name  string
			Value int
		}

		original := testStruct{Name: "test", Value: 123}
		require.NoError(t, c.Set("key3", original, time.Hour))

		val, err := c.Get("key3")
		require.NoError(t, err)
		assert.Equal(t, original, val)
	})

	t.Run("overwrite existing key", func(t *testing.T) {
		require.NoError(t, c.Set("key4", "first", time.Hour))
		require.NoError(t, c.Set("key4", "second", time.Hour))

		val, err := c.Get("key4")
		require.NoError(t, err)
		assert.Equal(t, "second", val)
	})
}

func TestCache_Get_NotFound(t *testing.T) {
	c := inmem.New[string, string]()

	val, err := c.Get("nonexistent")
	assert.ErrorIs(t, err, inmem.ErrNotFound)
	assert.Empty(t, val)
}

func TestCache_Get_Expired(t *testing.T) {
	c := inmem.New[string, string]()

	require.NoError(t, c.Set("expiring", "value", 50*time.Millisecond))

	val, err := c.Get("expiring")
	require.NoError(t, err)
	assert.Equal(t, "value", val)

	time.Sleep(100 * time.Millisecond)

	val, err = c.Get("expiring")
	assert.ErrorIs(t, err, inmem.ErrNotFound)
	assert.Empty(t, val)
}

func TestCache_Remove(t *testing.T) {
	c := inmem.New[string, string]()

	t.Run("single key", func(t *testing.T) {
		require.NoError(t, c.Set("remove1", "value", time.Hour))
		require.NoError(t, c.Remove("remove1"))

		_, err := c.Get("remove1")
		assert.ErrorIs(t, err, inmem.ErrNotFound)
	})

	t.Run("multiple keys", func(t *testing.T) {
		require.NoError(t, c.Set("remove2", "value2", time.Hour))
		require.NoError(t, c.Set("remove3", "value3", time.Hour))
		require.NoError(t, c.Remove("remove2", "remove3"))

		_, err := c.Get("remove2")
		assert.ErrorIs(t, err, inmem.ErrNotFound)
		_, err = c.Get("remove3")
		assert.ErrorIs(t, err, inmem.ErrNotFound)
	})

	t.Run("nonexistent key", func(t *testing.T) {
		require.NoError(t, c.Remove("nonexistent"))
	})

	t.Run("empty keys", func(t *testing.T) {
		require.NoError(t, c.Remove())
	})
}

func TestCache_Pop(t *testing.T) {
	c := inmem.New[string, string]()

	t.Run("existing key", func(t *testing.T) {
		require.NoError(t, c.Set("pop1", "popvalue", time.Hour))

		val, err := c.Pop("pop1")
		require.NoError(t, err)
		assert.Equal(t, "popvalue", val)

		_, err = c.Get("pop1")
		assert.ErrorIs(t, err, inmem.ErrNotFound)
	})

	t.Run("nonexistent key", func(t *testing.T) {
		val, err := c.Pop("nonexistent")
		assert.ErrorIs(t, err, inmem.ErrNotFound)
		assert.Empty(t, val)
	})
}

func TestCache_Keys(t *testing.T) {
	c := inmem.New[string, string]()

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
}

func TestCache_Add(t *testing.T) {
	c := inmem.New[string, any]()

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
		const contenders = 50
		results := make(chan bool, contenders)
		start := make(chan struct{})

		var wg sync.WaitGroup
		for i := range contenders {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				<-start
				won, err := c.Add("claim_race", id, time.Hour)
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

func TestCache_ConcurrentAccess(t *testing.T) {
	c := inmem.New[string, int]()

	const goroutines = 10
	const operations = 200

	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range operations {
				key := "concurrent:" + string(rune('A'+id)) + ":" + string(rune('0'+j%10))

				_ = c.Set(key, j, time.Hour)
				_, _ = c.Get(key)
				_ = c.Keys("concurrent:")
			}
		}(i)
	}
	wg.Wait()
}

func TestCache_ZeroTTL(t *testing.T) {
	c := inmem.New[string, string]()

	require.NoError(t, c.Set("zero_ttl", "value", 0))

	_, err := c.Get("zero_ttl")
	assert.ErrorIs(t, err, inmem.ErrNotFound)
}

func TestCache_NilValue(t *testing.T) {
	c := inmem.New[string, any]()

	require.NoError(t, c.Set("nilkey", nil, time.Hour))

	val, err := c.Get("nilkey")
	require.NoError(t, err)
	assert.Nil(t, val)
}
