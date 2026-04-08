package inmem

import (
	"testing"
	"time"

	"github.com/enverbisevac/libs/queue"
)

func TestReadyHeapOrdersByPriorityThenSeq(t *testing.T) {
	tp := newTopic()
	now := time.Now()
	tp.add(&item{id: "a", priority: queue.PriorityLow, runAt: now, seq: 1}, now)
	tp.add(&item{id: "b", priority: queue.PriorityHigh, runAt: now, seq: 2}, now)
	tp.add(&item{id: "c", priority: queue.PriorityNormal, runAt: now, seq: 3}, now)

	order := []string{}
	for {
		tp.mu.Lock()
		tp.promoteDue(now)
		it, _ := tp.claimReady(now, time.Minute)
		tp.mu.Unlock()
		if it == nil {
			break
		}
		order = append(order, it.id)
		// Simulate ack.
		tp.ack(it.id)
	}

	want := []string{"b", "c", "a"}
	if len(order) != len(want) {
		t.Fatalf("got %d items, want %d (%v)", len(order), len(want), order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order[%d] = %q, want %q (full: %v)", i, order[i], want[i], order)
		}
	}
}

func TestDelayedHeapPromotes(t *testing.T) {
	tp := newTopic()
	now := time.Unix(1000, 0)
	tp.add(&item{id: "soon", runAt: now.Add(10 * time.Second), seq: 1}, now)
	tp.add(&item{id: "later", runAt: now.Add(20 * time.Second), seq: 2}, now)

	tp.mu.Lock()
	tp.promoteDue(now)
	if it, _ := tp.claimReady(now, time.Minute); it != nil {
		t.Fatalf("nothing should be ready yet, got %q", it.id)
	}
	later := now.Add(15 * time.Second)
	tp.promoteDue(later)
	it, _ := tp.claimReady(later, time.Minute)
	if it == nil || it.id != "soon" {
		t.Fatalf("expected 'soon' to be ready, got %v", it)
	}
	tp.mu.Unlock()
}

func TestVisibilityReclaim(t *testing.T) {
	tp := newTopic()
	now := time.Unix(1000, 0)
	tp.add(&item{id: "x", priority: queue.PriorityNormal, runAt: now, seq: 1}, now)

	tp.mu.Lock()
	tp.promoteDue(now)
	it, attempt := tp.claimReady(now, time.Second)
	tp.mu.Unlock()
	if it == nil || it.id != "x" {
		t.Fatalf("first claim returned %v, want x", it)
	}
	if attempt != 1 {
		t.Fatalf("first claim attempt = %d, want 1", attempt)
	}

	// Second claim while still inflight: should be empty.
	tp.mu.Lock()
	tp.promoteDue(now)
	again, _ := tp.claimReady(now, time.Second)
	tp.mu.Unlock()
	if again != nil {
		t.Fatalf("inflight item must not be re-claimable, got %v", again)
	}

	// Advance past visibility deadline → reclaim.
	after := now.Add(2 * time.Second)
	tp.mu.Lock()
	tp.promoteDue(after)
	again, attempt = tp.claimReady(after, time.Second)
	tp.mu.Unlock()
	if again == nil || again.id != "x" {
		t.Fatalf("expected reclaim of x, got %v", again)
	}
	if attempt != 2 {
		t.Fatalf("reclaim attempt = %d, want 2", attempt)
	}
}
