package outbox

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// recordingPublisher captures every Publish call. It's a minimal Publisher
// implementation used to verify routing decisions.
type recordingPublisher struct {
	name string
	mu   sync.Mutex
	got  []Message
	err  error
}

func (p *recordingPublisher) Publish(_ context.Context, msgs ...Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.got = append(p.got, msgs...)
	return nil
}

func TestRouterWithRouteByTopic(t *testing.T) {
	events := &recordingPublisher{name: "events"}
	work := &recordingPublisher{name: "work"}

	router := NewRouter(
		WithRoute(func(m Message) Publisher {
			if strings.HasPrefix(m.Topic, "task.") {
				return work
			}
			return nil
		}),
		WithFallback(events),
	)

	msgs := []Message{
		{ID: "1", Topic: "user.created"},
		{ID: "2", Topic: "task.send_email"},
		{ID: "3", Topic: "order.placed"},
		{ID: "4", Topic: "task.charge_card"},
	}
	if err := router.Publish(context.Background(), msgs...); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	events.mu.Lock()
	defer events.mu.Unlock()
	work.mu.Lock()
	defer work.mu.Unlock()

	if len(events.got) != 2 || events.got[0].ID != "1" || events.got[1].ID != "3" {
		t.Errorf("events: ids = %v, want [1, 3]", ids(events.got))
	}
	if len(work.got) != 2 || work.got[0].ID != "2" || work.got[1].ID != "4" {
		t.Errorf("work: ids = %v, want [2, 4]", ids(work.got))
	}
}

func TestRouterWithDestination(t *testing.T) {
	events := &recordingPublisher{name: "events"}
	work := &recordingPublisher{name: "work"}

	router := NewRouter(
		WithDestination(map[string]Publisher{
			"queue":  work,
			"events": events,
		}),
		WithFallback(events),
	)

	msgs := []Message{
		{ID: "1", Topic: "user.created"}, // no header → fallback (events)
		{ID: "2", Topic: "send_email", Headers: map[string]string{HeaderDestination: "queue"}},
		{ID: "3", Topic: "user.deleted", Headers: map[string]string{HeaderDestination: "events"}},
		{ID: "4", Topic: "charge", Headers: map[string]string{HeaderDestination: "queue"}},
	}
	if err := router.Publish(context.Background(), msgs...); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	events.mu.Lock()
	defer events.mu.Unlock()
	work.mu.Lock()
	defer work.mu.Unlock()

	if len(events.got) != 2 || events.got[0].ID != "1" || events.got[1].ID != "3" {
		t.Errorf("events: ids = %v, want [1, 3]", ids(events.got))
	}
	if len(work.got) != 2 || work.got[0].ID != "2" || work.got[1].ID != "4" {
		t.Errorf("work: ids = %v, want [2, 4]", ids(work.got))
	}
}

func TestRouterWithHeadersCustomKey(t *testing.T) {
	a := &recordingPublisher{name: "a"}
	b := &recordingPublisher{name: "b"}

	router := NewRouter(
		WithHeaders("kind", map[string]Publisher{
			"task":  b,
			"event": a,
		}),
		WithFallback(a),
	)

	msgs := []Message{
		{ID: "1", Headers: map[string]string{"kind": "task"}},
		{ID: "2", Headers: map[string]string{"kind": "event"}},
		{ID: "3"}, // no headers → fallback (a)
	}
	if err := router.Publish(context.Background(), msgs...); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(a.got) != 2 || a.got[0].ID != "2" || a.got[1].ID != "3" {
		t.Errorf("a got %v, want [2, 3]", ids(a.got))
	}
	if len(b.got) != 1 || b.got[0].ID != "1" {
		t.Errorf("b got %v, want [1]", ids(b.got))
	}
}

func TestRouterRulesEvaluatedInOrder(t *testing.T) {
	first := &recordingPublisher{name: "first"}
	second := &recordingPublisher{name: "second"}

	// Both rules would match id=1; the first registered one must win.
	router := NewRouter(
		WithRoute(func(m Message) Publisher {
			if m.ID == "1" {
				return first
			}
			return nil
		}),
		WithRoute(func(m Message) Publisher {
			if m.ID == "1" || m.ID == "2" {
				return second
			}
			return nil
		}),
	)

	if err := router.Publish(context.Background(),
		Message{ID: "1"}, Message{ID: "2"},
	); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	first.mu.Lock()
	defer first.mu.Unlock()
	second.mu.Lock()
	defer second.mu.Unlock()
	if len(first.got) != 1 || first.got[0].ID != "1" {
		t.Errorf("first: %v, want [1]", ids(first.got))
	}
	if len(second.got) != 1 || second.got[0].ID != "2" {
		t.Errorf("second: %v, want [2]", ids(second.got))
	}
}

func TestRouterUnmatchedReturnsError(t *testing.T) {
	router := NewRouter(
		WithDestination(map[string]Publisher{}),
	)
	err := router.Publish(context.Background(), Message{ID: "1", Topic: "x"})
	if err == nil {
		t.Fatal("expected error for unmatched message with no fallback")
	}
	if !strings.Contains(err.Error(), "no publisher matched") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "no publisher matched")
	}
}

func TestRouterPropagatesPublishError(t *testing.T) {
	wantErr := errors.New("boom")
	bad := &recordingPublisher{err: wantErr}
	good := &recordingPublisher{}

	router := NewRouter(
		WithRoute(func(m Message) Publisher {
			if m.ID == "fail" {
				return bad
			}
			return good
		}),
	)

	err := router.Publish(context.Background(),
		Message{ID: "ok"},
		Message{ID: "fail"},
		Message{ID: "after"},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}

	// First message should have been delivered before the failure stopped the loop.
	good.mu.Lock()
	defer good.mu.Unlock()
	if len(good.got) != 1 || good.got[0].ID != "ok" {
		t.Errorf("good got %v, want [ok]", ids(good.got))
	}
}

func ids(msgs []Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID
	}
	return out
}
