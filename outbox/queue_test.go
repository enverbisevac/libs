package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/enverbisevac/libs/queue"
)

type mockEnqueuer struct {
	mu    sync.Mutex
	calls []enqueueCall
	err   error
}

type enqueueCall struct {
	topic   string
	payload []byte
}

func (m *mockEnqueuer) Enqueue(_ context.Context, topic string, payload []byte, _ ...queue.EnqueueOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.calls = append(m.calls, enqueueCall{topic: topic, payload: payload})
	return nil
}

func TestQueueAdapterPublish(t *testing.T) {
	mock := &mockEnqueuer{}
	adapter := NewQueueAdapter(mock)

	msgs := []Message{
		{ID: "1", Topic: "task.send_email", Payload: []byte("payload1")},
		{ID: "2", Topic: "task.charge_card", Payload: []byte("payload2")},
	}

	if err := adapter.Publish(context.Background(), msgs...); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(mock.calls))
	}

	if mock.calls[0].topic != "task.send_email" {
		t.Errorf("calls[0].topic = %q, want %q", mock.calls[0].topic, "task.send_email")
	}
	if string(mock.calls[0].payload) != "payload1" {
		t.Errorf("calls[0].payload = %q, want %q", mock.calls[0].payload, "payload1")
	}

	if mock.calls[1].topic != "task.charge_card" {
		t.Errorf("calls[1].topic = %q, want %q", mock.calls[1].topic, "task.charge_card")
	}
	if string(mock.calls[1].payload) != "payload2" {
		t.Errorf("calls[1].payload = %q, want %q", mock.calls[1].payload, "payload2")
	}
}

func TestQueueAdapterPublishError(t *testing.T) {
	enqErr := errors.New("queue down")
	mock := &mockEnqueuer{err: enqErr}
	adapter := NewQueueAdapter(mock)

	err := adapter.Publish(context.Background(), Message{ID: "1", Topic: "t", Payload: []byte("p")})
	if err == nil {
		t.Fatal("Publish() expected error")
	}
	if !errors.Is(err, enqErr) {
		t.Fatalf("Publish() error = %v, want %v", err, enqErr)
	}
}

func TestQueueAdapterEmpty(t *testing.T) {
	mock := &mockEnqueuer{}
	adapter := NewQueueAdapter(mock)

	if err := adapter.Publish(context.Background()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.calls) != 0 {
		t.Fatalf("calls = %d, want 0", len(mock.calls))
	}
}
