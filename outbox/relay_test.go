package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mockStore struct {
	mu            sync.Mutex
	messages      []Message
	processed     []string
	failed        map[string]error
	fetchErr      error
	markProcErr   error
	markFailedErr error
}

func newMockStore(msgs ...Message) *mockStore {
	return &mockStore{
		messages: msgs,
		failed:   make(map[string]error),
	}
}

func (m *mockStore) Save(_ context.Context, _ any, msgs ...Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockStore) FetchPending(_ context.Context, limit int) ([]Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	var result []Message
	count := 0
	for i := range m.messages {
		if m.messages[i].Status == StatusPending && count < limit {
			m.messages[i].Status = StatusProcessing
			result = append(result, m.messages[i])
			count++
		}
	}
	return result, nil
}

func (m *mockStore) MarkProcessed(_ context.Context, ids ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.markProcErr != nil {
		return m.markProcErr
	}
	m.processed = append(m.processed, ids...)
	for _, id := range ids {
		for i := range m.messages {
			if m.messages[i].ID == id {
				m.messages[i].Status = StatusProcessed
			}
		}
	}
	return nil
}

func (m *mockStore) MarkFailed(_ context.Context, id string, err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.markFailedErr != nil {
		return m.markFailedErr
	}
	m.failed[id] = err
	for i := range m.messages {
		if m.messages[i].ID == id {
			m.messages[i].Retries++
			m.messages[i].Status = StatusFailed
		}
	}
	return nil
}

func (m *mockStore) getProcessed() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.processed))
	copy(cp, m.processed)
	return cp
}

func (m *mockStore) getFailed() map[string]error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make(map[string]error, len(m.failed))
	for k, v := range m.failed {
		cp[k] = v
	}
	return cp
}

type mockPublisher struct {
	mu       sync.Mutex
	msgs     []Message
	err      error
	failOnID string
}

func (m *mockPublisher) Publish(_ context.Context, msgs ...Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, msg := range msgs {
		if m.failOnID != "" && msg.ID == m.failOnID {
			return m.err
		}
	}
	m.msgs = append(m.msgs, msgs...)
	return nil
}

func (m *mockPublisher) published() []Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]Message, len(m.msgs))
	copy(cp, m.msgs)
	return cp
}

func TestRelayProcessesMessages(t *testing.T) {
	store := newMockStore(
		Message{ID: "1", Topic: "orders", Payload: []byte("order1"), Status: StatusPending},
		Message{ID: "2", Topic: "orders", Payload: []byte("order2"), Status: StatusPending},
	)
	pub := &mockPublisher{}
	relay := NewRelay(store, pub, WithPollInterval(10*time.Millisecond))

	ctx := context.Background()
	relay.Start(ctx)

	// Wait for at least one poll cycle
	time.Sleep(50 * time.Millisecond)
	relay.Stop()

	published := pub.published()
	if len(published) != 2 {
		t.Fatalf("published %d messages, want 2", len(published))
	}

	processed := store.getProcessed()
	if len(processed) != 2 {
		t.Fatalf("processed %d messages, want 2", len(processed))
	}
}

func TestRelayMarksFailed(t *testing.T) {
	store := newMockStore(
		Message{ID: "1", Topic: "orders", Payload: []byte("ok"), Status: StatusPending},
		Message{ID: "2", Topic: "orders", Payload: []byte("fail"), Status: StatusPending},
	)
	pubErr := errors.New("publish error")
	pub := &mockPublisher{failOnID: "2", err: pubErr}
	relay := NewRelay(store, pub, WithPollInterval(10*time.Millisecond))

	ctx := context.Background()
	relay.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	relay.Stop()

	processed := store.getProcessed()
	if len(processed) != 1 {
		t.Fatalf("processed %d messages, want 1", len(processed))
	}
	if processed[0] != "1" {
		t.Fatalf("processed[0] = %q, want %q", processed[0], "1")
	}

	failed := store.getFailed()
	if len(failed) != 1 {
		t.Fatalf("failed %d messages, want 1", len(failed))
	}
	if failed["2"] == nil || failed["2"].Error() != pubErr.Error() {
		t.Fatalf("failed[2] = %v, want %v", failed["2"], pubErr)
	}
}

func TestRelayStartIdempotent(t *testing.T) {
	store := newMockStore()
	pub := &mockPublisher{}
	relay := NewRelay(store, pub, WithPollInterval(10*time.Millisecond))

	ctx := context.Background()
	relay.Start(ctx)
	relay.Start(ctx) // second call should be no-op
	relay.Stop()
}

func TestRelayStopWithoutStart(t *testing.T) {
	store := newMockStore()
	pub := &mockPublisher{}
	relay := NewRelay(store, pub)

	// Stop without Start should not panic
	relay.Stop()
}

func TestRelayNoMessages(t *testing.T) {
	store := newMockStore()
	pub := &mockPublisher{}
	relay := NewRelay(store, pub, WithPollInterval(10*time.Millisecond))

	ctx := context.Background()
	relay.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	relay.Stop()

	if len(pub.published()) != 0 {
		t.Fatal("expected no published messages")
	}
	if len(store.getProcessed()) != 0 {
		t.Fatal("expected no processed messages")
	}
}

func TestRelayRespectsContext(t *testing.T) {
	store := newMockStore()
	pub := &mockPublisher{}
	relay := NewRelay(store, pub, WithPollInterval(10*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	relay.Start(ctx)
	cancel()

	// Should finish quickly after context cancel
	done := make(chan struct{})
	go func() {
		relay.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay did not stop after context cancel")
	}
}

func TestRelayBatchSize(t *testing.T) {
	var msgs []Message
	for i := 0; i < 10; i++ {
		msgs = append(msgs, Message{
			ID:      string(rune('a' + i)),
			Topic:   "t",
			Payload: []byte("p"),
			Status:  StatusPending,
		})
	}
	store := newMockStore(msgs...)
	pub := &mockPublisher{}
	relay := NewRelay(store, pub,
		WithPollInterval(10*time.Millisecond),
		WithBatchSize(3),
	)

	ctx := context.Background()
	relay.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	relay.Stop()

	// With batch size 3, first poll should pick up at most 3
	// Subsequent polls pick more. After several cycles all should be processed.
	published := pub.published()
	if len(published) == 0 {
		t.Fatal("expected some published messages")
	}
}

func TestRelayFetchError(t *testing.T) {
	store := newMockStore()
	store.fetchErr = errors.New("db error")
	pub := &mockPublisher{}
	relay := NewRelay(store, pub, WithPollInterval(10*time.Millisecond))

	ctx := context.Background()
	relay.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	relay.Stop()

	// Should not panic and no messages published
	if len(pub.published()) != 0 {
		t.Fatal("expected no published messages on fetch error")
	}
}
