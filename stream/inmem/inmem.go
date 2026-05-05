package inmem

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/enverbisevac/libs/stream"
)

// ErrClosed is returned by Publish or Subscribe after Close.
var ErrClosed = errors.New("stream/inmem: service closed")

// Service is the in-memory stream.Service implementation.
type Service struct {
	cfg stream.ServiceConfig

	mu      sync.Mutex
	streams map[string]*streamState
	closed  bool

	// Track every live consumer so Service.Close can drain them.
	consumers []*consumer
}

// New returns a fresh Service configured with the given options.
func New(opts ...stream.ServiceOption) *Service {
	var cfg stream.ServiceConfig
	for _, o := range opts {
		o.Apply(&cfg)
	}
	stream.ResolveServiceConfig(&cfg)
	return &Service{
		cfg:     cfg,
		streams: make(map[string]*streamState),
	}
}

// streamFor returns (and creates if missing) the streamState for name.
// Caller must NOT hold s.mu.
func (s *Service) streamFor(name string) *streamState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[name]
	if !ok {
		st = newStreamState(name)
		s.streams[name] = st
	}
	return st
}

// Close releases backend resources and drains all live consumers, honoring
// each consumer's ShutdownTimeout. Safe to call once; subsequent calls
// return nil. ctx bounds the overall close: cancellation aborts before
// the next consumer is drained and returns ctx.Err() — already-drained
// consumers stay drained.
func (s *Service) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	consumers := append([]*consumer(nil), s.consumers...)
	s.consumers = nil
	s.mu.Unlock()

	for _, c := range consumers {
		if err := ctx.Err(); err != nil {
			return err
		}
		_ = c.Close()
	}
	return nil
}

// Publish appends a message to the named stream. Stream name is formatted
// as "<app>.<ns>.<name>" using service-level defaults overridable by
// WithPublishApp / WithPublishNamespace. Headers (if any) are shallow-
// copied. Trim policy (MaxLen / MaxAge) is applied after the append.
func (s *Service) Publish(_ context.Context, name string, payload []byte, opts ...stream.PublishOption) error {
	pcfg := stream.PublishConfig{
		App:       s.cfg.App,
		Namespace: s.cfg.Namespace,
		MaxLen:    s.cfg.DefaultMaxLen,
		MaxAge:    s.cfg.DefaultMaxAge,
	}
	for _, o := range opts {
		o.Apply(&pcfg)
	}
	formatted := stream.FormatStream(pcfg.App, pcfg.Namespace, name)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.mu.Unlock()

	st := s.streamFor(formatted)
	st.appendEntry(payload, pcfg.Headers, time.Now)
	st.applyTrim(pcfg.MaxLen, pcfg.MaxAge, time.Now())
	return nil
}

// LogLen returns the count of un-trimmed entries in the named stream's log.
// formatted is the result of FormatStream. Test helper; not part of the
// public stream.Service contract.
func (s *Service) LogLen(formatted string) int {
	s.mu.Lock()
	st, ok := s.streams[formatted]
	s.mu.Unlock()
	if !ok {
		return 0
	}
	return st.numEntries()
}

// Subscribe creates (if necessary) the consumer group and starts
// concurrency-many workers dispatching messages to h. The returned
// Consumer's Close stops dispatch and waits for in-flight handlers up to
// ShutdownTimeout.
func (s *Service) Subscribe(
	ctx context.Context, name, group string, h stream.Handler,
	opts ...stream.SubscribeOption,
) (stream.Consumer, error) {
	scfg := stream.SubscribeConfig{
		App:       s.cfg.App,
		Namespace: s.cfg.Namespace,
	}
	for _, o := range opts {
		o.Apply(&scfg)
	}
	formatted := stream.FormatStream(scfg.App, scfg.Namespace, name)
	stream.ResolveSubscribeConfig(&scfg, formatted)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	s.mu.Unlock()

	st := s.streamFor(formatted)

	// Resolve start seq under the stream lock so we see a consistent
	// nextSeq for "latest".
	st.mu.Lock()
	startSeq := st.resolveStartSeqLocked(scfg)
	g := st.orCreateGroupLocked(group, startSeq)
	st.mu.Unlock()

	c := &consumer{}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Worker/handler ctx is independent of the caller's ctx so that
	// canceling the request that called Subscribe does not stop the
	// consumer. Lifetime is controlled exclusively by Consumer.Close.
	workerCtx, workerCancel := context.WithCancel(context.Background())

	for i := 0; i < scfg.Concurrency; i++ {
		consumerName := fmt.Sprintf("%s-%d", group, i)
		wg.Go(func() {
			runWorker(workerCtx, st, g, h, scfg, consumerName, stop, s)
		})
	}

	c.closeFn = func() error {
		close(stop)
		workerCancel()
		// Workers select on stop in their wait loop; closing stop
		// preempts any in-progress wait. No cond broadcast needed
		// because the notify channel approach doesn't park goroutines.

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			return nil
		case <-time.After(scfg.ShutdownTimeout):
			return nil // orphaned in-flight handlers continue
		}
	}

	// Register the consumer under the lock and re-check closed: if Close
	// raced ahead while we were spawning workers, drain ourselves and
	// abort so we don't orphan goroutines outside Service.consumers.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = c.Close()
		return nil, ErrClosed
	}
	s.consumers = append(s.consumers, c)
	s.mu.Unlock()

	return c, nil
}

// consumer is the Consumer returned by Subscribe. Implemented in Task 11.
type consumer struct {
	closeOnce sync.Once
	closeFn   func() error
}

// Close stops claiming new messages and waits for in-flight handlers up to
// the configured ShutdownTimeout.
func (c *consumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		if c.closeFn != nil {
			err = c.closeFn()
		}
	})
	return err
}

// Compile-time check.
var _ stream.Service = (*Service)(nil)

// runWorker is the per-goroutine dispatch loop. It claims new entries
// past the group's cursor (visibility-timeout reclaim of expired pending
// entries is added in Task 11), runs the handler, and acks on success.
func runWorker(
	ctx context.Context,
	st *streamState,
	g *groupState,
	h stream.Handler,
	cfg stream.SubscribeConfig,
	consumerName string,
	stop <-chan struct{},
	svc *Service,
) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		batch, waker := claim(st, g, cfg, consumerName, time.Now())
		if len(batch) == 0 {
			timer := time.NewTimer(cfg.PollInterval)
			select {
			case <-waker:
			case <-timer.C:
			case <-stop:
			}
			timer.Stop()
			continue
		}

		for _, p := range batch {
			runOne(ctx, h, p, st, g, cfg, svc)
		}
	}
}

// claim atomically inspects the log and pending list and returns up to
// batchSize entries to dispatch under consumerName, plus a waker channel
// captured under the same lock. The waker fires when a future appendEntry
// closes the channel — capturing it here (vs after release) closes the
// race where a publish could land between claim returning empty and the
// worker entering its select.
//
// Reclaim happens FIRST: expired pending entries get a freshly allocated
// pendingEntry with the same source entry but a new identity, replacing
// the old one in g.pending. The identity-check in runOne (cur == p) then
// reliably distinguishes "still ours" from "reclaimed by someone else" —
// mutating the existing pendingEntry in place would defeat that check
// because the pointer in g.pending is unchanged.
//
// New entries are claimed second, up to the remaining batch capacity.
func claim(st *streamState, g *groupState, cfg stream.SubscribeConfig, consumerName string, now time.Time) ([]*pendingEntry, <-chan struct{}) {
	st.mu.Lock()
	defer st.mu.Unlock()

	const batchSize = 16
	out := make([]*pendingEntry, 0, batchSize)

	// 1. Reclaim expired pending entries first. Collect candidates then
	// replace, so we don't mutate the map under iteration.
	var expired []*pendingEntry
	for _, p := range g.pending {
		if len(expired) >= batchSize {
			break
		}
		if !p.claimDeadline.Before(now) {
			continue
		}
		expired = append(expired, p)
	}
	for _, old := range expired {
		np := &pendingEntry{
			entry:         old.entry,
			consumer:      consumerName,
			claimDeadline: now.Add(cfg.VisibilityTimeout),
			attempts:      old.attempts + 1,
		}
		g.pending[old.entry.seq] = np
		out = append(out, np)
	}

	// 2. Claim new entries past the cursor up to the remaining batch.
	for _, e := range st.nextEligibleLocked(g.lastSeq, batchSize-len(out)) {
		p := &pendingEntry{
			entry:         e,
			consumer:      consumerName,
			claimDeadline: now.Add(cfg.VisibilityTimeout),
			attempts:      1,
		}
		g.pending[e.seq] = p
		g.lastSeq = e.seq
		out = append(out, p)
	}
	return out, st.notifyCh
}

// runOne runs the handler against one pendingEntry and acks/retries.
func runOne(
	ctx context.Context,
	h stream.Handler,
	p *pendingEntry,
	st *streamState,
	g *groupState,
	cfg stream.SubscribeConfig,
	svc *Service,
) {
	// Snapshot the attempt count under the lock so claim()'s concurrent
	// mutation of p.attempts cannot race with our read.
	st.mu.Lock()
	attempt := p.attempts
	st.mu.Unlock()

	msg := &stream.Message{
		ID:        p.entry.id,
		Stream:    st.name,
		Payload:   p.entry.payload,
		Headers:   p.entry.headers,
		CreatedAt: p.entry.createdAt,
		Attempt:   attempt,
	}

	err := safeRun(ctx, h, msg)

	if err == nil {
		st.mu.Lock()
		// Only ack if the pending entry is still the one we're holding.
		// If another worker reclaimed it via the visibility-timeout path,
		// its claim is in-flight and must not be cleared by us.
		if cur := g.pending[p.entry.seq]; cur == p {
			delete(g.pending, p.entry.seq)
		}
		st.mu.Unlock()
		return
	}

	// Decide: route to DLQ (and ack) or leave in pending (so the next
	// claim cycle reclaims it).
	skipRetry := errors.Is(err, stream.ErrSkipRetry)
	overBudget := cfg.MaxRetries >= 0 && attempt > cfg.MaxRetries

	if skipRetry || overBudget {
		// Move to DLQ stream and remove from pending. Identity-check the
		// pending entry so we don't clobber a parallel reclaim.
		svc.publishToDLQ(p.entry, cfg.DeadLetterStream)
		st.mu.Lock()
		if cur := g.pending[p.entry.seq]; cur == p {
			delete(g.pending, p.entry.seq)
		}
		st.mu.Unlock()
		return
	}

	// Otherwise leave in pending; reclaim path picks it up after VT
	// expires and bumps attempts on the next claim.
}

// publishToDLQ enqueues entry into the named DLQ stream. dlqStream is the
// already-formatted stream name (e.g. "app.default.orders.dead").
// Headers and payload are preserved (cloned via appendEntry).
//
// Append-before-delete in the caller is intentional: at-least-once. If the
// DLQ append fails (impossible for inmem; possible in remote backends),
// the source message remains pending and reclaims, rather than vanishing.
//
// TODO(T22): preserve original message ID + source stream as headers so
// DLQ subscribers can correlate back to the source. The streamtest
// conformance suite is the right place to spec the header keys.
//
// TODO(T15): gate on s.closed (mirror Publish's ErrClosed return) so
// post-Close DLQ writes don't sneak in. The streamtest close-drain case
// will surface this if any backend doesn't gate.
func (s *Service) publishToDLQ(e *entry, dlqStream string) {
	st := s.streamFor(dlqStream)
	st.appendEntry(e.payload, e.headers, time.Now)
}

// safeRun runs the handler with a panic recovery wrapper. Panics are
// converted to errors so the worker doesn't tear down.
func safeRun(ctx context.Context, h stream.Handler, msg *stream.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("stream/inmem: handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, msg)
}
