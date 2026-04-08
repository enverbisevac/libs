package redis_test

import (
	"context"
	"os"
	"testing"

	rds "github.com/redis/go-redis/v9"

	"github.com/enverbisevac/libs/queue"
	"github.com/enverbisevac/libs/queue/queuetest"
	qredis "github.com/enverbisevac/libs/queue/redis"
)

// QUEUE_REDIS_ADDR must point at a Redis instance the test can FLUSH-DB on
// (e.g. localhost:6379, db 15).
func TestConformance(t *testing.T) {
	addr := os.Getenv("QUEUE_REDIS_ADDR")
	if addr == "" {
		t.Skip("QUEUE_REDIS_ADDR not set; skipping redis integration tests")
	}
	client := rds.NewClient(&rds.Options{Addr: addr, DB: 15})
	defer func() { _ = client.Close() }()
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	queuetest.Run(t, func(t *testing.T) (queue.Service, func()) {
		svc, err := qredis.New(context.Background(), client, queue.WithNamespace(t.Name()))
		if err != nil {
			t.Fatalf("new: %v", err)
		}
		return svc, func() { _ = svc.Close(context.Background()) }
	})
}
