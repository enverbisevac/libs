package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/enverbisevac/libs/pubsub"
)

type mockPubSubPublisher struct {
	mu    sync.Mutex
	calls []pubsubCall
	err   error
}

type pubsubCall struct {
	topic   string
	payload []byte
}

func (m *mockPubSubPublisher) Publish(_ context.Context, topic string, payload []byte, _ ...pubsub.PublishOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.calls = append(m.calls, pubsubCall{topic: topic, payload: payload})
	return nil
}

func TestPubSubAdapterPublish(t *testing.T) {
	mock := &mockPubSubPublisher{}
	adapter := NewPubSubAdapter(mock)

	msgs := []Message{
		{ID: "1", Topic: "orders", Payload: []byte("payload1")},
		{ID: "2", Topic: "events", Payload: []byte("payload2")},
	}

	if err := adapter.Publish(context.Background(), msgs...); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(mock.calls))
	}

	if mock.calls[0].topic != "orders" {
		t.Errorf("calls[0].topic = %q, want %q", mock.calls[0].topic, "orders")
	}
	if string(mock.calls[0].payload) != "payload1" {
		t.Errorf("calls[0].payload = %q, want %q", mock.calls[0].payload, "payload1")
	}

	if mock.calls[1].topic != "events" {
		t.Errorf("calls[1].topic = %q, want %q", mock.calls[1].topic, "events")
	}
	if string(mock.calls[1].payload) != "payload2" {
		t.Errorf("calls[1].payload = %q, want %q", mock.calls[1].payload, "payload2")
	}
}

func TestPubSubAdapterPublishError(t *testing.T) {
	pubErr := errors.New("broker unavailable")
	mock := &mockPubSubPublisher{err: pubErr}
	adapter := NewPubSubAdapter(mock)

	err := adapter.Publish(context.Background(), Message{ID: "1", Topic: "t", Payload: []byte("p")})
	if err == nil {
		t.Fatal("Publish() expected error")
	}
	if !errors.Is(err, pubErr) {
		t.Fatalf("Publish() error = %v, want %v", err, pubErr)
	}
}

func TestPubSubAdapterEmpty(t *testing.T) {
	mock := &mockPubSubPublisher{}
	adapter := NewPubSubAdapter(mock)

	if err := adapter.Publish(context.Background()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.calls) != 0 {
		t.Fatalf("calls = %d, want 0", len(mock.calls))
	}
}
