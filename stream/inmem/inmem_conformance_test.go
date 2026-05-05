package inmem_test

import (
	"context"
	"testing"

	"github.com/enverbisevac/libs/stream"
	"github.com/enverbisevac/libs/stream/inmem"
	"github.com/enverbisevac/libs/stream/streamtest"
)

func TestConformance(t *testing.T) {
	streamtest.Run(t, func(t *testing.T) (stream.Service, func()) {
		// Each sub-test gets its own Service so durable state cannot
		// bleed (relevant once sqlite/pgx/redis run the same suite).
		svc := inmem.New()
		return svc, func() { _ = svc.Close(context.Background()) }
	})
}
