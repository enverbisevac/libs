package inmem

import (
	"strings"
	"sync"
	"time"

	"github.com/enverbisevac/libs/cache"
)

// ErrNotFound is an alias for cache.ErrNotFound so existing callers using
// inmem.ErrNotFound keep working.
var ErrNotFound = cache.ErrNotFound

var (
	_ cache.Cache[string, any]   = (*Cache[string, any])(nil)
	_ cache.Claimer[string, any] = (*Cache[string, any])(nil)
)

type item[V any] struct {
	value  V
	expiry time.Time
}

func (i item[V]) isExpired() bool {
	return time.Now().After(i.expiry)
}

type Cache[K ~string, V any] struct {
	items map[K]item[V]
	mu    sync.RWMutex
}

func New[K ~string, V any]() *Cache[K, V] {
	c := &Cache[K, V]{
		items: make(map[K]item[V]),
	}

	go func() {
		for range time.Tick(5 * time.Second) {
			c.mu.Lock()
			for key, it := range c.items {
				if it.isExpired() {
					delete(c.items, key)
				}
			}
			c.mu.Unlock()
		}
	}()

	return c
}

func (c *Cache[K, V]) Set(key K, value V, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = item[V]{
		value:  value,
		expiry: time.Now().Add(ttl),
	}
	return nil
}

func (c *Cache[K, V]) Add(key K, value V, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, found := c.items[key]; found && !existing.isExpired() {
		return false, nil
	}

	c.items[key] = item[V]{
		value:  value,
		expiry: time.Now().Add(ttl),
	}
	return true, nil
}

func (c *Cache[K, V]) Get(key K) (V, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var zero V
	it, found := c.items[key]
	if !found {
		return zero, ErrNotFound
	}

	if it.isExpired() {
		delete(c.items, key)
		return zero, ErrNotFound
	}

	return it.value, nil
}

func (c *Cache[K, V]) Remove(keys ...K) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, key := range keys {
		delete(c.items, key)
	}
	return nil
}

func (c *Cache[K, V]) Pop(key K) (V, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var zero V
	it, found := c.items[key]
	if !found {
		return zero, ErrNotFound
	}

	delete(c.items, key)
	return it.value, nil
}

func (c *Cache[K, V]) Keys(prefix K) []K {
	c.mu.RLock()
	defer c.mu.RUnlock()

	output := make([]K, 0, len(c.items))
	for key := range c.items {
		if strings.HasPrefix(string(key), string(prefix)) {
			output = append(output, key)
		}
	}
	return output
}
