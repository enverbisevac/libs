package cache

import (
	"errors"
	"time"
)

// ErrNotFound is returned by backends when a key is missing or expired.
// Backends may alias this or wrap it; callers should check with errors.Is.
var ErrNotFound = errors.New("key not found")

type Cache[K ~string, V any] interface {
	Claimer[K, V]
	Set(key K, value V, ttl time.Duration) error
	Get(key K) (V, error)
	Remove(key ...K) error
	Pop(key K) (V, error)
	Keys(prefix K) []K
}

// Claimer is an optional capability for caches that can perform atomic
// set-if-absent. Backends that cannot provide this guarantee should not
// implement it.
//
// Add inserts key→value only if the key is absent or expired. It returns
// true if this caller won the claim, false if another caller already
// holds an unexpired entry. Use this for deduplication or leader-election
// where a Get-then-Set sequence would race.
type Claimer[K ~string, V any] interface {
	Add(key K, value V, ttl time.Duration) (bool, error)
}

type config struct {
	TTL           time.Duration
	AsyncSetter   bool
	OnSetterError func(err error)
}

type CacheConfigFunc func(*config)

var DefaultTTL = 1 * time.Hour

func TTL(duration time.Duration) CacheConfigFunc {
	return func(c *config) {
		c.TTL = duration
	}
}

func OnError(fn func(err error)) CacheConfigFunc {
	return func(c *config) {
		c.OnSetterError = fn
	}
}

func AsyncSetter(value bool) CacheConfigFunc {
	return func(c *config) {
		c.AsyncSetter = value
	}
}

func Get[K ~string, V any](
	cache Cache[K, V],
	key K,
	fetcher func() (V, error),
	options ...CacheConfigFunc,
) (V, error) {
	var (
		zero V
		c    config
	)

	for _, f := range options {
		f(&c)
	}

	if c.TTL == 0 {
		c.TTL = DefaultTTL
	}

	if value, err := cache.Get(key); err == nil {
		return value, nil
	} else if !errors.Is(err, ErrNotFound) {
		return zero, err
	}

	output, err := fetcher()
	if err != nil {
		return zero, err
	}

	setter := func() {
		if err := cache.Set(key, output, c.TTL); err != nil && c.OnSetterError != nil {
			c.OnSetterError(err)
		}
	}

	if c.AsyncSetter {
		go setter()
	} else {
		setter()
	}

	return output, nil
}
