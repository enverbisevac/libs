package redis

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/enverbisevac/libs/queue"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	rds "github.com/redis/go-redis/v9"
)

// Service is the Redis queue implementation.
type Service struct {
	cfg    queue.QueueConfig
	client rds.UniversalClient

	mu        sync.Mutex
	closed    bool
	consumers []*consumer

	enqueueSha string
	claimSha   string
	ackSha     string
	failSha    string
	reapSha    string
}

// New constructs a Redis queue service. The caller owns client.
func New(ctx context.Context, client rds.UniversalClient, opts ...queue.QueueOption) (*Service, error) {
	cfg := queue.QueueConfig{
		App:       queue.DefaultAppName,
		Namespace: queue.DefaultNamespace,
	}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	s := &Service{cfg: cfg, client: client}
	if err := s.loadScripts(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Service) loadScripts(ctx context.Context) error {
	for _, p := range []struct {
		dst    *string
		script string
		name   string
	}{
		{&s.enqueueSha, enqueueScript, "enqueue"},
		{&s.claimSha, claimScript, "claim"},
		{&s.ackSha, ackScript, "ack"},
		{&s.failSha, failScript, "fail"},
		{&s.reapSha, reapScript, "reap"},
	} {
		sha, err := s.client.ScriptLoad(ctx, p.script).Result()
		if err != nil {
			return fmt.Errorf("queue/redis: load %s script: %w", p.name, err)
		}
		*p.dst = sha
	}
	return nil
}

// Key layout uses Redis hash tags ({<topic>}) so all per-topic keys hash to
// the same slot in Redis Cluster mode. This is required because Lua scripts
// must touch only one slot per invocation.
func keyPrefix(topic string) string    { return "queue:{" + topic + "}" }
func readyKey(topic string) string     { return keyPrefix(topic) + ":ready" }
func inflightKey(topic string) string  { return keyPrefix(topic) + ":inflight" }
func jobKeyPrefix(topic string) string { return keyPrefix(topic) + ":job:" }
func jobKey(topic, id string) string   { return jobKeyPrefix(topic) + id }
func notifyKey(topic string) string    { return keyPrefix(topic) + ":notify" }

// Enqueue runs the enqueue Lua script.
func (s *Service) Enqueue(ctx context.Context, topicName string, payload []byte, opts ...queue.EnqueueOption) error {
	cfg := queue.EnqueueConfig{App: s.cfg.App, Namespace: s.cfg.Namespace}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	formatted := queue.FormatTopic(cfg.App, cfg.Namespace, topicName)
	now := time.Now()
	if cfg.RunAt.IsZero() {
		cfg.RunAt = now
	}
	id := uuid.NewString()
	_, err := s.client.EvalSha(ctx, s.enqueueSha,
		[]string{readyKey(formatted), jobKey(formatted, id), notifyKey(formatted)},
		id, payload, int(cfg.Priority), cfg.RunAt.UnixMilli(), now.UnixMilli(),
	).Result()
	if err != nil {
		return fmt.Errorf("queue/redis: enqueue: %w", err)
	}
	return nil
}

// Subscribe spawns a worker pool and a reaper goroutine.
func (s *Service) Subscribe(ctx context.Context, topicName string, h queue.Handler, opts ...queue.SubscribeOption) (queue.Consumer, error) {
	cfg := queue.SubscribeConfig{App: s.cfg.App, Namespace: s.cfg.Namespace}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	formatted := queue.FormatTopic(cfg.App, cfg.Namespace, topicName)
	queue.ResolveSubscribeConfig(&cfg, formatted)

	cctx, cancel := context.WithCancel(ctx)
	c := &consumer{
		svc:     s,
		cfg:     cfg,
		topic:   formatted,
		handler: h,
		ctx:     cctx,
		cancel:  cancel,
	}
	c.wg.Add(cfg.Concurrency + 1) // +1 for reaper
	for i := range cfg.Concurrency {
		go c.worker(i)
	}
	go c.reaper()
	s.mu.Lock()
	s.consumers = append(s.consumers, c)
	s.mu.Unlock()
	return c, nil
}

// Close closes all consumers.
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
	cfg     queue.SubscribeConfig
	topic   string
	handler queue.Handler

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
}

func (c *consumer) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		drained := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(drained)
		}()
		select {
		case <-drained:
		case <-time.After(c.cfg.ShutdownTimeout):
		}
	})
	return nil
}

func (c *consumer) worker(id int) {
	defer c.wg.Done()
	log := logr.FromContextOrDiscard(c.ctx).WithValues("topic", c.topic, "worker", id)
	for {
		if c.ctx.Err() != nil {
			return
		}
		j, attempt, err := c.claim()
		if err != nil {
			log.Error(err, "queue/redis: claim")
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if j != nil {
			c.process(j, attempt, log)
			continue
		}
		// Idle: BLPOP for wake or fall back to poll interval.
		_, _ = c.svc.client.BLPop(c.ctx, c.cfg.PollInterval, notifyKey(c.topic)).Result()
	}
}

func (c *consumer) claim() (*queue.Job, int, error) {
	now := time.Now().UnixMilli()
	res, err := c.svc.client.EvalSha(c.ctx, c.svc.claimSha,
		[]string{readyKey(c.topic), inflightKey(c.topic)},
		now, int(c.cfg.VisibilityTimeout/time.Millisecond), jobKeyPrefix(c.topic),
	).Result()
	if err != nil {
		if errors.Is(err, rds.Nil) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	if res == nil {
		return nil, 0, nil
	}
	arr, ok := res.([]any)
	if !ok || len(arr) != 4 {
		return nil, 0, fmt.Errorf("unexpected claim result: %T %v", res, res)
	}
	id, _ := arr[0].(string)
	payload, _ := arr[1].(string)
	attemptStr, _ := arr[2].(string)
	enqueuedStr, _ := arr[3].(string)
	attempt, _ := strconv.Atoi(attemptStr)
	enqueuedMs, _ := strconv.ParseInt(enqueuedStr, 10, 64)
	return &queue.Job{
		ID:         id,
		Topic:      c.topic,
		Payload:    []byte(payload),
		Attempt:    attempt,
		EnqueuedAt: time.UnixMilli(enqueuedMs),
	}, attempt, nil
}

func (c *consumer) process(j *queue.Job, attempt int, log logr.Logger) {
	margin := min(c.cfg.VisibilityTimeout/10, time.Second)
	hCtx, hCancel := context.WithDeadline(c.ctx, time.Now().Add(c.cfg.VisibilityTimeout-margin))
	defer hCancel()

	err := c.invoke(hCtx, j)
	if err == nil {
		_, aErr := c.svc.client.EvalSha(c.ctx, c.svc.ackSha,
			[]string{inflightKey(c.topic), jobKey(c.topic, j.ID)}, j.ID,
		).Result()
		if aErr != nil {
			log.Error(aErr, "queue/redis: ack", "id", j.ID)
		}
		return
	}
	log.Error(err, "queue/redis: handler error", "id", j.ID, "attempt", attempt)
	skip := errors.Is(err, queue.ErrSkipRetry)
	exhausted := c.cfg.MaxRetries >= 0 && attempt > c.cfg.MaxRetries
	if skip || exhausted {
		if dErr := c.routeDLQ(j); dErr != nil {
			log.Error(dErr, "queue/redis: DLQ; retaining", "id", j.ID)
			c.failBack(j, attempt, err, log)
			return
		}
		return
	}
	c.failBack(j, attempt, err, log)
}

func (c *consumer) invoke(ctx context.Context, j *queue.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("queue: handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return c.handler(ctx, j)
}

func (c *consumer) failBack(j *queue.Job, attempt int, hErr error, log logr.Logger) {
	delay := c.cfg.Backoff(attempt)
	runAt := time.Now().Add(delay).UnixMilli()
	// Read priority from hash to preserve it.
	prioStr, _ := c.svc.client.HGet(c.ctx, jobKey(c.topic, j.ID), "priority").Result()
	prio, _ := strconv.Atoi(prioStr)
	_, err := c.svc.client.EvalSha(c.ctx, c.svc.failSha,
		[]string{inflightKey(c.topic), readyKey(c.topic), jobKey(c.topic, j.ID)},
		j.ID, prio, runAt, hErr.Error(),
	).Result()
	if err != nil {
		log.Error(err, "queue/redis: failBack", "id", j.ID)
	}
}

// routeDLQ enqueues into the DLQ topic, then acks the source. The two
// operations are NOT atomic across topics — this is intentional so that the
// implementation works in Redis Cluster (each Lua script touches only one
// hash slot). If the second step fails, the source job will be re-claimed
// after its visibility expires and may produce a duplicate DLQ entry. This
// matches the package's at-least-once contract.
func (c *consumer) routeDLQ(j *queue.Job) error {
	newID := uuid.NewString()
	now := time.Now()
	// Step 1: enqueue into DLQ topic.
	_, err := c.svc.client.EvalSha(c.ctx, c.svc.enqueueSha,
		[]string{
			readyKey(c.cfg.DeadLetterTopic),
			jobKey(c.cfg.DeadLetterTopic, newID),
			notifyKey(c.cfg.DeadLetterTopic),
		},
		newID, j.Payload, int(queue.PriorityNormal), now.UnixMilli(), now.UnixMilli(),
	).Result()
	if err != nil {
		return fmt.Errorf("dlq enqueue: %w", err)
	}
	// Step 2: ack the source.
	if _, ackErr := c.svc.client.EvalSha(c.ctx, c.svc.ackSha,
		[]string{inflightKey(c.topic), jobKey(c.topic, j.ID)}, j.ID,
	).Result(); ackErr != nil {
		return fmt.Errorf("dlq ack source: %w", ackErr)
	}
	return nil
}

// reaper periodically scans inflight for expired leases and moves them back.
func (c *consumer) reaper() {
	defer c.wg.Done()
	log := logr.FromContextOrDiscard(c.ctx).WithValues("topic", c.topic, "role", "reaper")
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}
		now := time.Now().UnixMilli()
		_, err := c.svc.client.EvalSha(c.ctx, c.svc.reapSha,
			[]string{inflightKey(c.topic), readyKey(c.topic)}, now,
		).Result()
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error(err, "queue/redis: reap")
		}
	}
}
