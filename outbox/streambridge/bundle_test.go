package streambridge

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enverbisevac/libs/outbox"
	"github.com/enverbisevac/libs/stream"
)

// fakeStore is a minimal in-memory outbox.Store for bundle tests.
type fakeStore struct {
	mu      sync.Mutex
	pending []outbox.Message
	saved   []outbox.Message
	marks   []string
}

func (f *fakeStore) Save(_ context.Context, _ any, msgs ...outbox.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saved = append(f.saved, msgs...)
	f.pending = append(f.pending, msgs...)
	return nil
}

func (f *fakeStore) FetchPending(_ context.Context, limit int) ([]outbox.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pending) == 0 {
		return nil, nil
	}
	if limit > len(f.pending) {
		limit = len(f.pending)
	}
	out := append([]outbox.Message(nil), f.pending[:limit]...)
	f.pending = f.pending[limit:]
	return out, nil
}

func (f *fakeStore) MarkProcessed(_ context.Context, ids ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marks = append(f.marks, ids...)
	return nil
}

func (f *fakeStore) MarkFailed(context.Context, string, error) error { return nil }

// closableService is a thread-safe stream.Service stub for bundle tests.
type closableService struct {
	mu        sync.Mutex
	calls     []fakeCall
	publishCh chan struct{} // optional: receives one signal per Publish

	closed atomic.Int32
}

func (c *closableService) Publish(_ context.Context, name string, payload []byte, opts ...stream.PublishOption) error {
	cfg := stream.PublishConfig{}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	c.mu.Lock()
	c.calls = append(c.calls, fakeCall{
		stream:  name,
		payload: append([]byte(nil), payload...),
		cfg:     cfg,
	})
	c.mu.Unlock()
	if c.publishCh != nil {
		select {
		case c.publishCh <- struct{}{}:
		default:
		}
	}
	return nil
}

func (c *closableService) Subscribe(context.Context, string, string, stream.Handler, ...stream.SubscribeOption) (stream.Consumer, error) {
	panic("not used")
}

func (c *closableService) Close(context.Context) error {
	c.closed.Add(1)
	return nil
}

func (c *closableService) callsSnapshot() []fakeCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]fakeCall(nil), c.calls...)
}

func TestBundle_New_PopulatesAllFields(t *testing.T) {
	store := &fakeStore{}
	svc := &closableService{}

	b := New(store, svc)

	if b.Store != store {
		t.Errorf("Store not set")
	}
	if b.Stream != svc {
		t.Errorf("Stream not set")
	}
	if b.Relay == nil {
		t.Errorf("Relay not set")
	}
}

func TestBundle_Save_ForwardsToStore(t *testing.T) {
	store := &fakeStore{}
	svc := &closableService{}
	b := New(store, svc)

	msg := outbox.Message{ID: "x", Topic: "t", Payload: []byte("p")}
	if err := b.Save(context.Background(), nil, msg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if got := len(store.saved); got != 1 || store.saved[0].ID != "x" {
		t.Errorf("store.saved = %+v, want [x]", store.saved)
	}
}

func TestBundle_Publish_ForwardsToStream(t *testing.T) {
	store := &fakeStore{}
	svc := &closableService{}
	b := New(store, svc)

	if err := b.Publish(context.Background(), "topic", []byte("payload")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := len(svc.callsSnapshot()); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestBundle_Stop_ClosesStreamAndRunsClosersInReverse(t *testing.T) {
	store := &fakeStore{}
	svc := &closableService{}

	var order []int
	b := New(store, svc)
	b.closers = []func() error{
		func() error { order = append(order, 1); return nil },
		func() error { order = append(order, 2); return nil },
	}

	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := svc.closed.Load(); got != 1 {
		t.Errorf("Stream.Close calls = %d, want 1", got)
	}
	if len(order) != 2 || order[0] != 2 || order[1] != 1 {
		t.Errorf("closer order = %v, want [2 1]", order)
	}
}

func TestBundle_Stop_JoinsErrors(t *testing.T) {
	store := &fakeStore{}
	svc := &closableService{}
	b := New(store, svc)
	b.closers = []func() error{
		func() error { return errors.New("a") },
		func() error { return errors.New("b") },
	}

	err := b.Stop(context.Background())
	if err == nil {
		t.Fatal("Stop: expected joined error")
	}
	msg := err.Error()
	if !contains(msg, "a") || !contains(msg, "b") {
		t.Errorf("error = %q, want both 'a' and 'b'", msg)
	}
}

func TestBundle_RelayDrainsStoreThroughBridge(t *testing.T) {
	store := &fakeStore{}
	svc := &closableService{publishCh: make(chan struct{}, 1)}
	// Pre-load the store with one pending message so the relay has work.
	_ = store.Save(context.Background(), nil, outbox.Message{
		ID: "1", Topic: "t", Payload: []byte("hello"),
	})

	b := New(store, svc, outbox.WithPollInterval(20*time.Millisecond))
	b.Start(context.Background())
	t.Cleanup(func() { _ = b.Stop(context.Background()) })

	select {
	case <-svc.publishCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected the relay to publish via the bridge")
	}
	calls := svc.callsSnapshot()
	if len(calls) == 0 || calls[0].stream != "t" {
		t.Errorf("calls = %+v, want first stream 't'", calls)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Compile-time interface check: streambridge.Publisher implements outbox.Publisher.
var _ stream.Service = (*closableService)(nil)
