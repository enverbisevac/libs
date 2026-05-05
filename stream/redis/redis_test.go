package redis_test

import (
	"context"
	"testing"

	rds "github.com/redis/go-redis/v9"

	"github.com/enverbisevac/libs/internal/testdb"
	"github.com/enverbisevac/libs/stream"
	sredis "github.com/enverbisevac/libs/stream/redis"
	"github.com/enverbisevac/libs/stream/streamtest"
)

// TestConformance uses STREAM_REDIS_ADDR if set, otherwise spins up an
// ephemeral redis testcontainer. Skips if neither is available.
func TestConformance(t *testing.T) {
	addr := testdb.Redis(t, "STREAM_REDIS_ADDR")

	client := rds.NewClient(&rds.Options{Addr: addr, DB: 15})
	defer func() { _ = client.Close() }()
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	streamtest.Run(t, func(t *testing.T) (stream.Service, func()) {
		svc := sredis.New(client, stream.WithNamespace(t.Name()))
		return svc, func() { _ = svc.Close(context.Background()) }
	})
}
