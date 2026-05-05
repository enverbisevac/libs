package inmem_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/enverbisevac/libs/stream"
	"github.com/enverbisevac/libs/stream/inmem"
	"github.com/stretchr/testify/require"
)

func TestHeadersRoundTrip(t *testing.T) {
	svc := inmem.New()
	defer func() { _ = svc.Close(context.Background()) }()

	ctx := context.Background()

	var (
		mu      sync.Mutex
		gotHdrs map[string]string
	)
	cons, err := svc.Subscribe(ctx, "orders", "billing",
		func(_ context.Context, m *stream.Message) error {
			mu.Lock()
			gotHdrs = m.Headers
			mu.Unlock()
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	hdrs := map[string]string{
		"trace-id":     "abc123",
		"content-type": "application/json",
	}
	require.NoError(t, svc.Publish(ctx, "orders", []byte(`{}`),
		stream.WithHeaders(hdrs)))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotHdrs != nil
	}, 2*time.Second, 5*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "abc123", gotHdrs["trace-id"])
	require.Equal(t, "application/json", gotHdrs["content-type"])
}

func TestHeaders_ShallowCopiedAtPublish(t *testing.T) {
	svc := inmem.New()
	defer func() { _ = svc.Close(context.Background()) }()

	ctx := context.Background()

	var (
		mu      sync.Mutex
		gotHdrs map[string]string
	)
	cons, err := svc.Subscribe(ctx, "orders", "billing",
		func(_ context.Context, m *stream.Message) error {
			mu.Lock()
			gotHdrs = m.Headers
			mu.Unlock()
			return nil
		},
		stream.WithStartFrom(stream.StartEarliest),
		stream.WithPollInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = cons.Close() }()

	hdrs := map[string]string{"trace-id": "v1"}
	require.NoError(t, svc.Publish(ctx, "orders", []byte("a"),
		stream.WithHeaders(hdrs)))

	// Publisher mutates after publish.
	hdrs["trace-id"] = "v2"

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotHdrs != nil
	}, 2*time.Second, 5*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "v1", gotHdrs["trace-id"], "headers must be snapshotted at publish time")
}
