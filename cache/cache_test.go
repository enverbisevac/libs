package cache_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enverbisevac/libs/cache"
	"github.com/enverbisevac/libs/cache/inmem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingCache wraps an inmem.Cache and lets tests inject errors on Get/Set.
type failingCache struct {
	inner  *inmem.Cache[string, string]
	getErr error
	setErr error
}

func (f *failingCache) Set(key, value string, ttl time.Duration) error {
	if f.setErr != nil {
		return f.setErr
	}
	return f.inner.Set(key, value, ttl)
}

func (f *failingCache) Get(key string) (string, error) {
	if f.getErr != nil {
		return "", f.getErr
	}
	return f.inner.Get(key)
}

func (f *failingCache) Add(key, value string, ttl time.Duration) (bool, error) {
	return f.inner.Add(key, value, ttl)
}

func (f *failingCache) Remove(keys ...string) error { return f.inner.Remove(keys...) }
func (f *failingCache) Pop(key string) (string, error) {
	return f.inner.Pop(key)
}
func (f *failingCache) Keys(prefix string) []string { return f.inner.Keys(prefix) }

func TestGet_CacheHit_SkipsFetcher(t *testing.T) {
	c := inmem.New[string, string]()
	require.NoError(t, c.Set("hit", "cached", time.Hour))

	var fetcherCalls int32
	val, err := cache.Get(c, "hit", func() (string, error) {
		atomic.AddInt32(&fetcherCalls, 1)
		return "fetched", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "cached", val)
	assert.Equal(t, int32(0), atomic.LoadInt32(&fetcherCalls), "fetcher should not run on hit")
}

func TestGet_CacheMiss_CallsFetcherAndCaches(t *testing.T) {
	c := inmem.New[string, string]()

	var fetcherCalls int32
	val, err := cache.Get(c, "miss", func() (string, error) {
		atomic.AddInt32(&fetcherCalls, 1)
		return "fetched", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "fetched", val)
	assert.Equal(t, int32(1), atomic.LoadInt32(&fetcherCalls))

	// Subsequent call should hit the cache.
	val, err = cache.Get(c, "miss", func() (string, error) {
		atomic.AddInt32(&fetcherCalls, 1)
		return "should-not-run", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "fetched", val)
	assert.Equal(t, int32(1), atomic.LoadInt32(&fetcherCalls))
}

func TestGet_FetcherError_Propagates(t *testing.T) {
	c := inmem.New[string, string]()

	wantErr := errors.New("fetch failed")
	val, err := cache.Get(c, "err", func() (string, error) {
		return "", wantErr
	})

	assert.ErrorIs(t, err, wantErr)
	assert.Empty(t, val)

	_, err = c.Get("err")
	assert.ErrorIs(t, err, cache.ErrNotFound, "failed fetch should not populate cache")
}

func TestGet_NonNotFoundCacheError_Propagates(t *testing.T) {
	inner := inmem.New[string, string]()
	wantErr := errors.New("redis down")
	c := &failingCache{inner: inner, getErr: wantErr}

	var fetcherCalls int32
	val, err := cache.Get(c, "key", func() (string, error) {
		atomic.AddInt32(&fetcherCalls, 1)
		return "fetched", nil
	})

	assert.ErrorIs(t, err, wantErr)
	assert.Empty(t, val)
	assert.Equal(t, int32(0), atomic.LoadInt32(&fetcherCalls), "fetcher should not run on cache error")
}

func TestGet_TTLOption_Applied(t *testing.T) {
	c := inmem.New[string, string]()

	val, err := cache.Get(c, "ttl-key", func() (string, error) {
		return "val", nil
	}, cache.TTL(50*time.Millisecond))

	require.NoError(t, err)
	assert.Equal(t, "val", val)

	// Value is cached.
	cached, err := c.Get("ttl-key")
	require.NoError(t, err)
	assert.Equal(t, "val", cached)

	// After TTL, entry expires.
	time.Sleep(100 * time.Millisecond)
	_, err = c.Get("ttl-key")
	assert.ErrorIs(t, err, cache.ErrNotFound)
}

func TestGet_DefaultTTL_UsedWhenUnset(t *testing.T) {
	c := inmem.New[string, string]()

	origDefault := cache.DefaultTTL
	cache.DefaultTTL = 50 * time.Millisecond
	t.Cleanup(func() { cache.DefaultTTL = origDefault })

	_, err := cache.Get(c, "default-ttl", func() (string, error) {
		return "val", nil
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	_, err = c.Get("default-ttl")
	assert.ErrorIs(t, err, cache.ErrNotFound)
}

func TestGet_OnError_CalledOnSetFailure(t *testing.T) {
	wantErr := errors.New("write failed")
	c := &failingCache{inner: inmem.New[string, string](), setErr: wantErr}

	var captured error
	val, err := cache.Get(c, "key",
		func() (string, error) { return "fetched", nil },
		cache.OnError(func(e error) { captured = e }),
	)

	require.NoError(t, err, "Get returns fetched value even when Set fails")
	assert.Equal(t, "fetched", val)
	assert.ErrorIs(t, captured, wantErr)
}

func TestGet_AsyncSetter_ReturnsBeforeSet(t *testing.T) {
	c := inmem.New[string, string]()

	val, err := cache.Get(c, "async", func() (string, error) {
		return "fetched", nil
	}, cache.AsyncSetter(true))

	require.NoError(t, err)
	assert.Equal(t, "fetched", val)

	// Allow the goroutine to complete.
	assert.Eventually(t, func() bool {
		cached, err := c.Get("async")
		return err == nil && cached == "fetched"
	}, time.Second, 10*time.Millisecond)
}

func TestGet_AsyncSetter_OnErrorCalled(t *testing.T) {
	wantErr := errors.New("write failed")
	c := &failingCache{inner: inmem.New[string, string](), setErr: wantErr}

	var captured atomic.Value
	val, err := cache.Get(c, "async-err",
		func() (string, error) { return "fetched", nil },
		cache.AsyncSetter(true),
		cache.OnError(func(e error) { captured.Store(e) }),
	)

	require.NoError(t, err)
	assert.Equal(t, "fetched", val)

	assert.Eventually(t, func() bool {
		v := captured.Load()
		if v == nil {
			return false
		}
		return errors.Is(v.(error), wantErr)
	}, time.Second, 10*time.Millisecond)
}
