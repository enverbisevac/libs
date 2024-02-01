package redis

import (
	"context"
	"time"

	"github.com/enverbisevac/libs/cache"
	"github.com/redis/go-redis/v9"
)

var (
	DefaultOperationTimeout = 10 * time.Second
)

var (
	_ cache.Cache = (*Cache)(nil)
)

type Cache struct {
	client redis.UniversalClient
}

func (c *Cache) Set(key string, value any, duration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	return c.client.Set(ctx, key, value, duration).Err()
}

func (c *Cache) Get(key string) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()
	var value any
	err := c.client.Get(ctx, key).Scan(value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (c *Cache) Remove(keys ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	return c.client.Del(ctx, keys...).Err()
}

func (c *Cache) Pop(key string) (any, error) {
	value, err := c.Get(key)
	if err != nil {
		return nil, err
	}
	err = c.Remove(key)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (c *Cache) Keys(prefix string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	output, err := c.client.Keys(ctx, prefix).Result()
	if err != nil {
		return []string{}
	}
	return output
}
