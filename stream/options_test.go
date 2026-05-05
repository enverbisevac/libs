package stream_test

import (
	"testing"
	"time"

	"github.com/enverbisevac/libs/stream"
	"github.com/stretchr/testify/require"
)

func TestServiceOptions(t *testing.T) {
	var cfg stream.ServiceConfig
	stream.WithApp("myapp").Apply(&cfg)
	stream.WithNamespace("tenant1").Apply(&cfg)
	stream.WithDefaultMaxLen(1000).Apply(&cfg)
	stream.WithDefaultMaxAge(time.Hour).Apply(&cfg)

	require.Equal(t, "myapp", cfg.App)
	require.Equal(t, "tenant1", cfg.Namespace)
	require.EqualValues(t, 1000, cfg.DefaultMaxLen)
	require.Equal(t, time.Hour, cfg.DefaultMaxAge)
}

func TestServiceOptions_EmptyStringIgnored(t *testing.T) {
	cfg := stream.ServiceConfig{App: "preset"}
	stream.WithApp("").Apply(&cfg)
	require.Equal(t, "preset", cfg.App, "empty string must not overwrite preset")
}

func TestPublishOptions(t *testing.T) {
	var cfg stream.PublishConfig
	cfg.MaxLen = 0 // service default
	cfg.MaxAge = 0

	stream.WithHeaders(map[string]string{"trace-id": "abc"}).Apply(&cfg)
	stream.WithMaxLen(500).Apply(&cfg)
	stream.WithMaxAge(2 * time.Hour).Apply(&cfg)
	stream.WithPublishApp("override-app").Apply(&cfg)
	stream.WithPublishNamespace("override-ns").Apply(&cfg)

	require.Equal(t, "abc", cfg.Headers["trace-id"])
	require.EqualValues(t, 500, cfg.MaxLen)
	require.Equal(t, 2*time.Hour, cfg.MaxAge)
	require.Equal(t, "override-app", cfg.App)
	require.Equal(t, "override-ns", cfg.Namespace)
}

func TestSubscribeOptions(t *testing.T) {
	var cfg stream.SubscribeConfig

	stream.WithStartFrom(stream.StartEarliest).Apply(&cfg)
	stream.WithStartFromID("42").Apply(&cfg)
	stream.WithConcurrency(4).Apply(&cfg)
	stream.WithMaxRetries(7).Apply(&cfg)
	stream.WithDeadLetterStream("custom.dlq").Apply(&cfg)
	stream.WithPollInterval(250 * time.Millisecond).Apply(&cfg)
	stream.WithVisibilityTimeout(10 * time.Second).Apply(&cfg)
	stream.WithShutdownTimeout(5 * time.Second).Apply(&cfg)
	stream.WithSubscribeApp("override-app").Apply(&cfg)
	stream.WithSubscribeNamespace("override-ns").Apply(&cfg)

	require.Equal(t, stream.StartEarliest, cfg.StartFrom)
	require.Equal(t, "42", cfg.StartFromID)
	require.Equal(t, 4, cfg.Concurrency)
	require.Equal(t, 7, cfg.MaxRetries)
	require.Equal(t, "custom.dlq", cfg.DeadLetterStream)
	require.Equal(t, 250*time.Millisecond, cfg.PollInterval)
	require.Equal(t, 10*time.Second, cfg.VisibilityTimeout)
	require.Equal(t, 5*time.Second, cfg.ShutdownTimeout)
	require.Equal(t, "override-app", cfg.App)
	require.Equal(t, "override-ns", cfg.Namespace)
}

func TestSubscribeOptions_ConcurrencyClamped(t *testing.T) {
	var cfg stream.SubscribeConfig
	stream.WithConcurrency(0).Apply(&cfg)
	require.Equal(t, 1, cfg.Concurrency, "0 must clamp to 1")

	stream.WithConcurrency(-5).Apply(&cfg)
	require.Equal(t, 1, cfg.Concurrency, "negative must clamp to 1")
}

func TestResolveSubscribeConfig_Defaults(t *testing.T) {
	formatted := stream.FormatStream("app", "default", "orders")
	cfg := stream.SubscribeConfig{}
	stream.ResolveSubscribeConfig(&cfg, formatted)

	require.Equal(t, 1, cfg.Concurrency)
	require.Equal(t, stream.DefaultMaxRetries, cfg.MaxRetries)
	require.Equal(t, stream.DefaultPollInterval, cfg.PollInterval)
	require.Equal(t, stream.DefaultVisibilityTimeout, cfg.VisibilityTimeout)
	require.Equal(t, stream.DefaultShutdownTimeout, cfg.ShutdownTimeout)
	require.Equal(t, stream.DeadLetterStream(formatted), cfg.DeadLetterStream)
}

func TestResolveSubscribeConfig_PreservesUserValues(t *testing.T) {
	cfg := stream.SubscribeConfig{
		App:               "myapp",
		Namespace:         "tenant1",
		Concurrency:       8,
		MaxRetries:        99,
		PollInterval:      time.Millisecond,
		VisibilityTimeout: time.Second,
		ShutdownTimeout:   2 * time.Second,
		DeadLetterStream:  "x.y.z",
	}
	stream.ResolveSubscribeConfig(&cfg, "app.default.orders")
	require.Equal(t, "myapp", cfg.App)
	require.Equal(t, "tenant1", cfg.Namespace)
	require.Equal(t, 8, cfg.Concurrency)
	require.Equal(t, 99, cfg.MaxRetries)
	require.Equal(t, time.Millisecond, cfg.PollInterval)
	require.Equal(t, time.Second, cfg.VisibilityTimeout)
	require.Equal(t, 2*time.Second, cfg.ShutdownTimeout)
	require.Equal(t, "x.y.z", cfg.DeadLetterStream)
}

func TestResolveSubscribeConfig_PreservesInfiniteRetries(t *testing.T) {
	cfg := stream.SubscribeConfig{MaxRetries: -1}
	stream.ResolveSubscribeConfig(&cfg, "app.default.orders")
	require.Equal(t, -1, cfg.MaxRetries, "-1 sentinel (infinite retries) must survive Resolve")
}
