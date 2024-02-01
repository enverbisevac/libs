package cache

import "time"

type Cache interface {
	Set(key string, value any, ttl time.Duration) error
	Get(key string) (any, error)
	Remove(key ...string) error
	Pop(key string) (any, error)
	Keys(prefix string) []string
}
