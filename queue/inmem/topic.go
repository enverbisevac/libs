package inmem

import (
	"container/heap"
	"sync"
	"time"

	"github.com/enverbisevac/libs/queue"
)

// item is one queued job inside a topic. The same struct lives in either the
// delayed heap or the ready heap depending on its state.
type item struct {
	id         string
	payload    []byte
	priority   queue.Priority
	attempt    int
	enqueuedAt time.Time
	runAt      time.Time
	seq        uint64
	// inflight is set while a worker is running the handler. It blocks the
	// item from being delivered twice and is cleared on success/failure.
	inflight bool
	// lockedUntil is the visibility deadline while inflight.
	lockedUntil time.Time
}

// delayedHeap orders by runAt ascending: top = "next to become ready".
type delayedHeap []*item

func (h delayedHeap) Len() int { return len(h) }
func (h delayedHeap) Less(i, j int) bool {
	if !h[i].runAt.Equal(h[j].runAt) {
		return h[i].runAt.Before(h[j].runAt)
	}
	return h[i].seq < h[j].seq
}
func (h delayedHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *delayedHeap) Push(x any)   { *h = append(*h, x.(*item)) }
func (h *delayedHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// readyHeap orders by priority DESC, then seq ASC. Top = "next to deliver".
type readyHeap []*item

func (h readyHeap) Len() int { return len(h) }
func (h readyHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	return h[i].seq < h[j].seq
}
func (h readyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *readyHeap) Push(x any)   { *h = append(*h, x.(*item)) }
func (h *readyHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// topic owns the two heaps + an inflight map for one topic, plus a Cond used
// to wake workers.
//
// State machine: every item is in EXACTLY ONE of {delayed, ready, inflight}
// at any moment. claim() pops from ready and inserts into inflight. ack
// removes from inflight. fail moves from inflight back to delayed (or ready
// if the new runAt is already past). promoteDue moves due items from delayed
// to ready and reclaims expired inflight items back to ready/delayed.
type topic struct {
	mu       sync.Mutex
	cond     *sync.Cond
	delayed  delayedHeap
	ready    readyHeap
	inflight map[string]*item
}

func newTopic() *topic {
	t := &topic{inflight: make(map[string]*item)}
	t.cond = sync.NewCond(&t.mu)
	return t
}

// add inserts an item into delayed or ready depending on whether runAt is in
// the future. Caller must hold no lock; this method takes t.mu.
func (t *topic) add(it *item, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if it.runAt.After(now) {
		heap.Push(&t.delayed, it)
	} else {
		heap.Push(&t.ready, it)
	}
	t.cond.Broadcast()
}

// promoteDue moves due delayed items into ready and reclaims any inflight
// items whose visibility deadline has passed (back into ready or delayed
// depending on their stored runAt). Caller must hold t.mu.
func (t *topic) promoteDue(now time.Time) {
	moved := false
	for t.delayed.Len() > 0 && !t.delayed[0].runAt.After(now) {
		it := heap.Pop(&t.delayed).(*item)
		heap.Push(&t.ready, it)
		moved = true
	}
	// Reclaim expired inflight items.
	for id, it := range t.inflight {
		if it.lockedUntil.After(now) {
			continue
		}
		delete(t.inflight, id)
		it.inflight = false
		it.lockedUntil = time.Time{}
		if it.runAt.After(now) {
			heap.Push(&t.delayed, it)
		} else {
			heap.Push(&t.ready, it)
		}
		moved = true
	}
	if moved {
		t.cond.Broadcast()
	}
}

// nextDelayedAt returns the soonest runAt among delayed items, or zero if
// none. Caller must hold t.mu.
func (t *topic) nextDelayedAt() time.Time {
	if t.delayed.Len() == 0 {
		return time.Time{}
	}
	return t.delayed[0].runAt
}

// claimReady pops the highest-priority eligible item, increments its attempt
// counter, marks it inflight, inserts it into the inflight map, and returns
// (item, attempt). Returns (nil, 0) if nothing is ready. Caller must hold
// t.mu. The attempt counter is mutated only under this lock so concurrent
// workers cannot race on it.
func (t *topic) claimReady(now time.Time, visibility time.Duration) (*item, int) {
	if t.ready.Len() == 0 {
		return nil, 0
	}
	it := heap.Pop(&t.ready).(*item)
	it.attempt++
	it.inflight = true
	it.lockedUntil = now.Add(visibility)
	t.inflight[it.id] = it
	return it, it.attempt
}

// ack removes an item from inflight (success path). Caller must hold no lock.
func (t *topic) ack(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.inflight, id)
}

// fail moves an inflight item back to delayed/ready with a new runAt. Caller
// must hold no lock.
func (t *topic) fail(id string, runAt time.Time, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	it, ok := t.inflight[id]
	if !ok {
		// Already reclaimed by promoteDue — nothing to do.
		return
	}
	delete(t.inflight, id)
	it.inflight = false
	it.lockedUntil = time.Time{}
	it.runAt = runAt
	if runAt.After(now) {
		heap.Push(&t.delayed, it)
	} else {
		heap.Push(&t.ready, it)
	}
	t.cond.Broadcast()
}
