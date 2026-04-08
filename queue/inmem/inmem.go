package inmem

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/enverbisevac/libs/queue"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
)

// Service is the in-memory queue.Service implementation.
type Service struct {
	cfg queue.QueueConfig

	mu     sync.Mutex
	topics map[string]*topic
	seq    atomic.Uint64
	closed bool

	// consumers tracks active subscriptions for orderly shutdown.
	consumers []*consumer
}

// New creates an in-memory queue service.
func New(opts ...queue.QueueOption) *Service {
	cfg := queue.QueueConfig{
		App:       queue.DefaultAppName,
		Namespace: queue.DefaultNamespace,
	}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	return &Service{
		cfg:    cfg,
		topics: make(map[string]*topic),
	}
}

func (s *Service) topicFor(name string) *topic {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.topics[name]; ok {
		return t
	}
	t := newTopic()
	s.topics[name] = t
	return t
}

// Enqueue places a job onto the queue. The "topic" argument is the user-facing
// short name; it is formatted with app/namespace before storage.
func (s *Service) Enqueue(_ context.Context, topicName string, payload []byte, opts ...queue.EnqueueOption) error {
	if s.isClosed() {
		return errors.New("queue/inmem: service closed")
	}
	cfg := queue.EnqueueConfig{App: s.cfg.App, Namespace: s.cfg.Namespace}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	formatted := queue.FormatTopic(cfg.App, cfg.Namespace, topicName)
	now := time.Now()
	if cfg.RunAt.IsZero() {
		cfg.RunAt = now
	}
	it := &item{
		id:         uuid.NewString(),
		payload:    append([]byte(nil), payload...),
		priority:   cfg.Priority,
		attempt:    0,
		enqueuedAt: now,
		runAt:      cfg.RunAt,
		seq:        s.seq.Add(1),
	}
	s.topicFor(formatted).add(it, now)
	return nil
}

func (s *Service) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Subscribe registers a worker pool against a topic.
func (s *Service) Subscribe(ctx context.Context, topicName string, h queue.Handler, opts ...queue.SubscribeOption) (queue.Consumer, error) {
	cfg := queue.SubscribeConfig{App: s.cfg.App, Namespace: s.cfg.Namespace}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	formatted := queue.FormatTopic(cfg.App, cfg.Namespace, topicName)
	queue.ResolveSubscribeConfig(&cfg, formatted)

	t := s.topicFor(formatted)
	cctx, cancel := context.WithCancel(ctx)
	c := &consumer{
		svc:     s,
		topic:   t,
		name:    formatted,
		cfg:     cfg,
		handler: h,
		ctx:     cctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	c.wg.Add(cfg.Concurrency)
	for i := range cfg.Concurrency {
		go c.worker(i)
	}
	// Plus one timer goroutine to wake the cond when delayed jobs become due.
	go c.delayedTimer()

	s.mu.Lock()
	s.consumers = append(s.consumers, c)
	s.mu.Unlock()
	return c, nil
}

// Close closes all consumers and marks the service as closed.
func (s *Service) Close(_ context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	consumers := append([]*consumer(nil), s.consumers...)
	s.mu.Unlock()
	for _, c := range consumers {
		_ = c.Close()
	}
	return nil
}

type consumer struct {
	svc     *Service
	topic   *topic
	name    string
	cfg     queue.SubscribeConfig
	handler queue.Handler

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	done   chan struct{}

	closeOnce sync.Once
}

func (c *consumer) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		// Wake all workers blocked on cond.
		c.topic.cond.L.Lock()
		c.topic.cond.Broadcast()
		c.topic.cond.L.Unlock()
		// Wait up to ShutdownTimeout for workers to drain.
		drained := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(drained)
		}()
		select {
		case <-drained:
		case <-time.After(c.cfg.ShutdownTimeout):
			// Workers are still running. Document: handlers exceeding shutdown
			// continue as orphan goroutines.
		}
		close(c.done)
	})
	return nil
}

// delayedTimer wakes the topic cond whenever a delayed job becomes due.
func (c *consumer) delayedTimer() {
	for {
		c.topic.mu.Lock()
		next := c.topic.nextDelayedAt()
		c.topic.mu.Unlock()
		var wait time.Duration
		if next.IsZero() {
			wait = c.cfg.PollInterval
		} else {
			wait = max(time.Until(next), 0)
		}
		t := time.NewTimer(wait)
		select {
		case <-c.ctx.Done():
			t.Stop()
			return
		case <-t.C:
			c.topic.cond.L.Lock()
			c.topic.cond.Broadcast()
			c.topic.cond.L.Unlock()
		}
	}
}

// worker is one goroutine in the worker pool. It claims items and runs the
// handler with panic recovery.
func (c *consumer) worker(id int) {
	defer c.wg.Done()
	log := logr.FromContextOrDiscard(c.ctx).WithValues("topic", c.name, "worker", id)

	for {
		c.topic.mu.Lock()
		for {
			if c.ctx.Err() != nil {
				c.topic.mu.Unlock()
				return
			}
			now := time.Now()
			c.topic.promoteDue(now)
			it, attempt := c.topic.claimReady(now, c.cfg.VisibilityTimeout)
			if it != nil {
				// Snapshot the immutable parts of the item *under the lock*
				// so subsequent races (e.g. another worker reclaiming the
				// inflight item after vis expires) cannot mutate the values
				// the handler observes.
				snap := jobSnap{
					id:         it.id,
					payload:    it.payload,
					attempt:    attempt,
					enqueuedAt: it.enqueuedAt,
				}
				c.topic.mu.Unlock()
				c.process(snap, log)
				break
			}
			// Nothing ready — wait for a broadcast.
			c.topic.cond.Wait()
		}
	}
}

// jobSnap is the immutable snapshot of an item handed to a worker. It exists
// because *item is shared state under the topic mutex; the worker holds no
// pointer to it after release of the mutex.
type jobSnap struct {
	id         string
	payload    []byte
	attempt    int
	enqueuedAt time.Time
}

func (c *consumer) process(snap jobSnap, log logr.Logger) {
	job := &queue.Job{
		ID:         snap.id,
		Topic:      c.name,
		Payload:    snap.payload,
		Attempt:    snap.attempt,
		EnqueuedAt: snap.enqueuedAt,
	}
	// Per-handler context with deadline = visibility - safety.
	margin := min(c.cfg.VisibilityTimeout/10, time.Second)
	hCtx, hCancel := context.WithDeadline(c.ctx, time.Now().Add(c.cfg.VisibilityTimeout-margin))
	defer hCancel()

	err := callHandler(hCtx, c.handler, job)
	if err == nil {
		c.topic.ack(snap.id)
		return
	}
	log.Error(err, "queue/inmem: handler error", "id", snap.id, "attempt", snap.attempt)

	// Permanent failure or budget exhausted → DLQ.
	skip := errors.Is(err, queue.ErrSkipRetry)
	exhausted := c.cfg.MaxRetries >= 0 && snap.attempt > c.cfg.MaxRetries
	if skip || exhausted {
		// Route to DLQ via the same Service.Enqueue path. The DLQ topic name
		// is already formatted because cfg.DeadLetterTopic was resolved with
		// the formatted topic.
		if dlqErr := c.svc.enqueueFormatted(c.cfg.DeadLetterTopic, snap.payload, queue.PriorityNormal); dlqErr != nil {
			log.Error(dlqErr, "queue/inmem: DLQ enqueue failed; retaining job", "id", snap.id)
			// Leave the job in retry state instead of losing it.
			c.topic.fail(snap.id, time.Now().Add(c.cfg.Backoff(snap.attempt)), time.Now())
			return
		}
		c.topic.ack(snap.id)
		return
	}
	// Otherwise reschedule with backoff.
	c.topic.fail(snap.id, time.Now().Add(c.cfg.Backoff(snap.attempt)), time.Now())
}

// callHandler runs the handler with panic recovery. A panic is converted to a
// regular error so the worker goroutine survives.
func callHandler(ctx context.Context, h queue.Handler, j *queue.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("queue: handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, j)
}

// enqueueFormatted is the internal enqueue path used by DLQ routing. It
// bypasses topic formatting because the DLQ topic name is already formatted.
func (s *Service) enqueueFormatted(formatted string, payload []byte, prio queue.Priority) error {
	if s.isClosed() {
		return errors.New("queue/inmem: service closed")
	}
	now := time.Now()
	it := &item{
		id:         uuid.NewString(),
		payload:    append([]byte(nil), payload...),
		priority:   prio,
		attempt:    0,
		enqueuedAt: now,
		runAt:      now,
		seq:        s.seq.Add(1),
	}
	s.topicFor(formatted).add(it, now)
	return nil
}
