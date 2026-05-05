// Package testdb starts ephemeral Postgres/Redis containers for integration
// tests. Each helper accepts an env-var name; when that variable is set it is
// returned verbatim (caller-supplied infrastructure wins). Otherwise a
// testcontainers-go container is started and registered for cleanup.
//
// If neither path produces a usable backend (e.g. Docker is unavailable),
// the test is skipped with a clear message — the same outcome as the legacy
// "skip when env var unset" pattern, but with automatic local containers
// when Docker is running and CI doesn't need to set env vars.
package testdb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Postgres returns a Postgres DSN. Resolution order:
//  1. If envVar is non-empty and the env var is set, return its value.
//  2. Otherwise start an ephemeral postgres testcontainer and register
//     cleanup. The DSN points at the new container.
//  3. If the container can't start (e.g. Docker unavailable), t.Skip.
func Postgres(t *testing.T, envVar string) string {
	t.Helper()
	if envVar != "" {
		if v := os.Getenv(envVar); v != "" {
			return v
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Skipf("testdb: cannot start postgres container (set %s to bypass): %v", envVar, err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()
		_ = c.Terminate(stopCtx)
	})

	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("testdb: connection string: %v", err)
	}
	return dsn
}

// Redis returns a Redis address (host:port). Resolution order matches
// Postgres above: env var → testcontainer → t.Skip.
func Redis(t *testing.T, envVar string) string {
	t.Helper()
	if envVar != "" {
		if v := os.Getenv(envVar); v != "" {
			return v
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Skipf("testdb: cannot start redis container (set %s to bypass): %v", envVar, err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()
		_ = c.Terminate(stopCtx)
	})

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("testdb: host: %v", err)
	}
	port, err := c.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatalf("testdb: port: %v", err)
	}
	return host + ":" + port.Port()
}
