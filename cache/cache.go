package cache

import "time"

type Cache interface {
	Set(key string, value any, ttl time.Duration) error
	Get(key string) (any, error)
	Remove(key ...string) error
	Pop(key string) (any, error)
	Keys(prefix string) []string
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

func Get[T any](cache Cache, key string, fetcher func() (T, error), options ...CacheConfigFunc) (T, error) {
	var (
		zero T
		c    config
	)

	for _, f := range options {
		f(&c)
	}

	if c.TTL == 0 {
		c.TTL = DefaultTTL
	}

	if value, err := cache.Get(key); err == nil && value != nil {
		output, ok := value.(T)
		if ok {
			return output, nil
		}
	}

	output, err := fetcher()
	if err != nil {
		return zero, err
	}

	setter := func() {
		err = cache.Set(key, output, c.TTL)
		if err != nil {
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
