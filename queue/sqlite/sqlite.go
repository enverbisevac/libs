package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/enverbisevac/libs/queue"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
)

//go:embed schema.sql
var Schema string

// Apply executes the embedded schema against db. Idempotent — uses CREATE
// TABLE IF NOT EXISTS.
func Apply(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, Schema)
	return err
}

// Service is the SQLite queue implementation.
type Service struct {
	cfg queue.QueueConfig
	db  *sql.DB

	mu        sync.Mutex
	closed    bool
	consumers []*consumer
	// wakeCh is broadcast on Enqueue so local workers wake immediately
	// instead of waiting for the next poll.
	wakeMu sync.Mutex
	wake   map[string]chan struct{}
}

// New constructs a SQLite queue.Service. The caller owns db.
func New(db *sql.DB, opts ...queue.QueueOption) *Service {
	cfg := queue.QueueConfig{
		App:       queue.DefaultAppName,
		Namespace: queue.DefaultNamespace,
	}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	return &Service{
		cfg:  cfg,
		db:   db,
		wake: make(map[string]chan struct{}),
	}
}

func (s *Service) wakeChan(topic string) chan struct{} {
	s.wakeMu.Lock()
	defer s.wakeMu.Unlock()
	ch, ok := s.wake[topic]
	if !ok {
		ch = make(chan struct{}, 1)
		s.wake[topic] = ch
	}
	return ch
}

func (s *Service) signal(topic string) {
	ch := s.wakeChan(topic)
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Enqueue inserts a job into queue_jobs.
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
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO queue_jobs (id, topic, payload, priority, attempt, run_at, enqueued_at)
		 VALUES (?, ?, ?, ?, 0, ?, ?)`,
		id, formatted, payload, int(cfg.Priority), cfg.RunAt.UnixMilli(), now.UnixMilli(),
	)
	if err != nil {
		return fmt.Errorf("queue/sqlite: insert: %w", err)
	}
	s.signal(formatted)
	return nil
}

// Subscribe spawns a worker pool that polls + reacts to local enqueue signals.
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
	c.wg.Add(cfg.Concurrency)
	for i := range cfg.Concurrency {
		go c.worker(i)
	}
	s.mu.Lock()
	s.consumers = append(s.consumers, c)
	s.mu.Unlock()
	return c, nil
}

// Close shuts down all consumers.
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
	wake := c.svc.wakeChan(c.topic)
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if c.ctx.Err() != nil {
			return
		}
		// Try to claim one job.
		job, attempt, err := c.claim()
		if err != nil {
			log.Error(err, "queue/sqlite: claim")
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if job != nil {
			c.process(job, attempt, log)
			continue
		}
		// Idle wait.
		select {
		case <-c.ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

// claim runs the BEGIN IMMEDIATE → SELECT → UPDATE pattern. Returns the job
// and its attempt (post-increment) or (nil, 0, nil) when nothing is ready.
func (c *consumer) claim() (*queue.Job, int, error) {
	now := time.Now()
	tx, err := c.svc.db.BeginTx(c.ctx, &sql.TxOptions{})
	if err != nil {
		return nil, 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(c.ctx, `
		SELECT id, payload, attempt, enqueued_at
		  FROM queue_jobs
		 WHERE topic = ?
		   AND run_at <= ?
		   AND (locked_until IS NULL OR locked_until < ?)
		 ORDER BY priority DESC, run_at ASC, enqueued_at ASC
		 LIMIT 1`,
		c.topic, now.UnixMilli(), now.UnixMilli(),
	)
	var (
		id         string
		payload    []byte
		attempt    int
		enqueuedMs int64
	)
	if err := row.Scan(&id, &payload, &attempt, &enqueuedMs); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("select: %w", err)
	}
	newAttempt := attempt + 1
	lockUntil := now.Add(c.cfg.VisibilityTimeout).UnixMilli()
	if _, err := tx.ExecContext(c.ctx,
		`UPDATE queue_jobs SET locked_until = ?, attempt = ? WHERE id = ?`,
		lockUntil, newAttempt, id,
	); err != nil {
		return nil, 0, fmt.Errorf("update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, fmt.Errorf("commit: %w", err)
	}
	return &queue.Job{
		ID:         id,
		Topic:      c.topic,
		Payload:    payload,
		Attempt:    newAttempt,
		EnqueuedAt: time.UnixMilli(enqueuedMs),
	}, newAttempt, nil
}

func (c *consumer) process(j *queue.Job, attempt int, log logr.Logger) {
	margin := min(c.cfg.VisibilityTimeout/10, time.Second)
	hCtx, hCancel := context.WithDeadline(c.ctx, time.Now().Add(c.cfg.VisibilityTimeout-margin))
	defer hCancel()

	err := c.invoke(hCtx, j)
	if err == nil {
		if _, derr := c.svc.db.ExecContext(c.ctx, `DELETE FROM queue_jobs WHERE id = ?`, j.ID); derr != nil {
			log.Error(derr, "queue/sqlite: delete on success", "id", j.ID)
		}
		return
	}
	log.Error(err, "queue/sqlite: handler error", "id", j.ID, "attempt", attempt)
	skip := errors.Is(err, queue.ErrSkipRetry)
	exhausted := c.cfg.MaxRetries >= 0 && attempt > c.cfg.MaxRetries
	if skip || exhausted {
		if dErr := c.routeDLQ(j); dErr != nil {
			log.Error(dErr, "queue/sqlite: DLQ insert; retaining job", "id", j.ID)
			c.reschedule(j, attempt, err)
			return
		}
		return
	}
	c.reschedule(j, attempt, err)
}

func (c *consumer) invoke(ctx context.Context, j *queue.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("queue: handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return c.handler(ctx, j)
}

// reschedule clears the lock and bumps run_at by the backoff.
func (c *consumer) reschedule(j *queue.Job, attempt int, hErr error) {
	delay := c.cfg.Backoff(attempt)
	runAt := time.Now().Add(delay).UnixMilli()
	_, err := c.svc.db.ExecContext(c.ctx,
		`UPDATE queue_jobs SET locked_until = NULL, run_at = ?, last_error = ? WHERE id = ?`,
		runAt, hErr.Error(), j.ID,
	)
	if err != nil {
		logr.FromContextOrDiscard(c.ctx).Error(err, "queue/sqlite: reschedule", "id", j.ID)
	}
}

// routeDLQ atomically inserts the job into the DLQ topic and deletes the
// original. Both operations happen in one transaction so a failure leaves the
// original visible for retry.
func (c *consumer) routeDLQ(j *queue.Job) error {
	tx, err := c.svc.db.BeginTx(c.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now()
	newID := uuid.NewString()
	if _, err := tx.ExecContext(c.ctx,
		`INSERT INTO queue_jobs (id, topic, payload, priority, attempt, run_at, enqueued_at)
		 VALUES (?, ?, ?, 0, 0, ?, ?)`,
		newID, c.cfg.DeadLetterTopic, j.Payload, now.UnixMilli(), now.UnixMilli(),
	); err != nil {
		return fmt.Errorf("dlq insert: %w", err)
	}
	if _, err := tx.ExecContext(c.ctx, `DELETE FROM queue_jobs WHERE id = ?`, j.ID); err != nil {
		return fmt.Errorf("dlq delete original: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	c.svc.signal(c.cfg.DeadLetterTopic)
	return nil
}
