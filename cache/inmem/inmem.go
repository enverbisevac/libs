package inmem

import (
	"errors"
	"sync"
	"time"

	"github.com/enverbisevac/libs/cache"
)

var (
	ErrNotFound = errors.New("key not found")
)

var (
	_ cache.Cache = (*Cache)(nil)
)

// item represents a cache item with a value and an expiration time.
type item struct {
	value  any
	expiry time.Time
}

// isExpired checks if the cache item has expired.
func (i item) isExpired() bool {
	return time.Now().After(i.expiry)
}

// Cache is a generic cache implementation with support for time-to-live
// (TTL) expiration.
type Cache struct {
	items map[string]item // The map storing cache items.
	mu    sync.Mutex      // Mutex for controlling concurrent access to the cache.
}

// NewTTL creates a new TTLCache instance and starts a goroutine to periodically
// remove expired items every 5 seconds.
func New() *Cache {
	c := &Cache{
		items: make(map[string]item),
	}

	go func() {
		for range time.Tick(5 * time.Second) {
			c.mu.Lock()

			// Iterate over the cache items and delete expired ones.
			for key, item := range c.items {
				if item.isExpired() {
					delete(c.items, key)
				}
			}

			c.mu.Unlock()
		}
	}()

	return c
}

// Set adds a new item to the cache with the specified key, value, and
// time-to-live (TTL).
func (c *Cache) Set(key string, value any, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = item{
		value:  value,
		expiry: time.Now().Add(ttl),
	}
	return nil
}

// Get retrieves the value associated with the given key from the cache.
func (c *Cache) Get(key string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, found := c.items[key]
	if !found {
		// If the key is not found, return the zero value for V and false.
		return nil, ErrNotFound
	}

	if item.isExpired() {
		// If the item has expired, remove it from the cache and return the zero
		// value for V and false.
		delete(c.items, key)
		return nil, ErrNotFound
	}

	// If the item is still valid, return its value and true.
	return item.value, nil
}

// Remove removes the item with the specified key from the cache.
func (c *Cache) Remove(keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Delete the item with the given key from the cache.
	for _, key := range keys {
		delete(c.items, key)
	}
	return nil
}

// Pop removes and returns the item with the specified key from the cache.
func (c *Cache) Pop(key string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, found := c.items[key]
	if !found {
		// If the key is not found, return the zero value for V and false.
		return item.value, ErrNotFound
	}

	// If the key is found, delete the item from the cache and return its value
	// and true.
	delete(c.items, key)
	return item.value, nil
}
