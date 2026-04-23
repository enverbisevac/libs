package redis

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"time"

	"github.com/enverbisevac/libs/cache"
	"github.com/redis/go-redis/v9"
)

var DefaultOperationTimeout = 10 * time.Second

// ErrNotFound aliases cache.ErrNotFound for callers using redis.ErrNotFound.
var ErrNotFound = cache.ErrNotFound

var (
	_ cache.Cache[string, any]   = (*Cache[string, any])(nil)
	_ cache.Claimer[string, any] = (*Cache[string, any])(nil)
)

type Cache[K ~string, V any] struct {
	client redis.UniversalClient
}

func New[K ~string, V any](client redis.UniversalClient) *Cache[K, V] {
	return &Cache[K, V]{
		client: client,
	}
}

func encode[V any](value V) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode[V any](data []byte) (V, error) {
	var value V
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&value); err != nil {
		return value, err
	}
	return value, nil
}

func (c *Cache[K, V]) Set(key K, value V, duration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	data, err := encode(value)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, string(key), data, duration).Err()
}

func (c *Cache[K, V]) Add(key K, value V, duration time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	data, err := encode(value)
	if err != nil {
		return false, err
	}
	return c.client.SetNX(ctx, string(key), data, duration).Result()
}

func (c *Cache[K, V]) Get(key K) (V, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	var zero V
	data, err := c.client.Get(ctx, string(key)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return zero, ErrNotFound
		}
		return zero, err
	}
	return decode[V](data)
}

func (c *Cache[K, V]) Remove(keys ...K) error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	strKeys := make([]string, len(keys))
	for i, k := range keys {
		strKeys[i] = string(k)
	}
	return c.client.Del(ctx, strKeys...).Err()
}

func (c *Cache[K, V]) Pop(key K) (V, error) {
	value, err := c.Get(key)
	if err != nil {
		return value, err
	}
	if err := c.Remove(key); err != nil {
		var zero V
		return zero, err
	}
	return value, nil
}

func (c *Cache[K, V]) Keys(prefix K) []K {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOperationTimeout)
	defer cancel()

	rows, err := c.client.Keys(ctx, string(prefix)).Result()
	if err != nil {
		return []K{}
	}
	output := make([]K, len(rows))
	for i, r := range rows {
		output[i] = K(r)
	}
	return output
}
