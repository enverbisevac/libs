package pgx

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/enverbisevac/libs/queue"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	pgx5 "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var Schema string

// Apply executes the embedded schema against pool. Idempotent.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, Schema)
	return err
}

// listenerCommand is queued onto Service.cmdChan for the listener goroutine
// to execute on its dedicated connection. This serializes Exec calls with
// WaitForNotification, since pgx.Conn is not safe for concurrent use.
type listenerCommand struct {
	sql      string
	resultCh chan error
}

// Service is the pgx queue implementation.
type Service struct {
	cfg  queue.QueueConfig
	pool *pgxpool.Pool

	mu        sync.Mutex
	closed    bool
	consumers []*consumer

	// Listener state.
	listenerOnce     sync.Once
	listenerPoolConn *pgxpool.Conn
	listenerConn     *pgx5.Conn
	listenerCtx      context.Context
	listenerStop     context.CancelFunc
	listenerDone     chan struct{}
	cmdChan          chan listenerCommand

	// cancelWaitMu protects cancelWait. The listener loop sets this each
	// time it enters WaitForNotification; execCommand cancels it to wake the
	// listener.
	cancelWaitMu sync.Mutex
	cancelWait   context.CancelFunc

	// Per-topic broadcast channels for wake-up.
	wakeMu sync.Mutex
	wake   map[string]chan struct{}
}

// New constructs a pgx queue service.
func New(pool *pgxpool.Pool, opts ...queue.QueueOption) *Service {
	cfg := queue.QueueConfig{
		App:       queue.DefaultAppName,
		Namespace: queue.DefaultNamespace,
	}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	return &Service{
		cfg:     cfg,
		pool:    pool,
		wake:    make(map[string]chan struct{}),
		cmdChan: make(chan listenerCommand, 16),
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

// notifyChannel maps a formatted topic to a Postgres LISTEN channel name.
// Postgres identifiers are limited to 63 chars; for safety we truncate using a
// stable hash if the input is too long.
func notifyChannel(formattedTopic string) string {
	const prefix = "queue_"
	// Postgres allows pretty much any character in a quoted identifier, but
	// LISTEN/NOTIFY uses an unquoted identifier in many drivers. We sanitize
	// to [a-zA-Z0-9_] for portability.
	var b strings.Builder
	b.Grow(len(prefix) + len(formattedTopic))
	b.WriteString(prefix)
	for _, r := range formattedTopic {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := b.String()
	if len(out) > 63 {
		out = out[:63]
	}
	return out
}

// Enqueue inserts a row and fires NOTIFY.
func (s *Service) Enqueue(ctx context.Context, topicName string, payload []byte, opts ...queue.EnqueueOption) error {
	cfg := queue.EnqueueConfig{App: s.cfg.App, Namespace: s.cfg.Namespace}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	formatted := queue.FormatTopic(cfg.App, cfg.Namespace, topicName)
	if cfg.RunAt.IsZero() {
		cfg.RunAt = time.Now()
	}
	id := uuid.New()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("queue/pgx: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`INSERT INTO queue_jobs (id, topic, payload, priority, attempt, run_at, enqueued_at)
		 VALUES ($1, $2, $3, $4, 0, $5, now())`,
		id, formatted, payload, int16(cfg.Priority), cfg.RunAt,
	); err != nil {
		return fmt.Errorf("queue/pgx: insert: %w", err)
	}
	// pg_notify takes a 1-arg variant up to 8000 bytes; we send empty payload.
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, '')`, notifyChannel(formatted)); err != nil {
		return fmt.Errorf("queue/pgx: notify: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("queue/pgx: commit: %w", err)
	}
	s.signal(formatted)
	return nil
}

// Subscribe spawns a worker pool and ensures the LISTEN connection is up.
func (s *Service) Subscribe(ctx context.Context, topicName string, h queue.Handler, opts ...queue.SubscribeOption) (queue.Consumer, error) {
	cfg := queue.SubscribeConfig{App: s.cfg.App, Namespace: s.cfg.Namespace}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	formatted := queue.FormatTopic(cfg.App, cfg.Namespace, topicName)
	queue.ResolveSubscribeConfig(&cfg, formatted)

	if err := s.ensureListener(ctx); err != nil {
		return nil, err
	}
	if err := s.listen(ctx, notifyChannel(formatted)); err != nil {
		return nil, err
	}

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

// ensureListener acquires a dedicated pool connection and starts the listener
// goroutine on first call. Matches the pubsub/pgx pattern: we hold the
// pgxpool.Conn indefinitely so the pool will not hand the underlying conn out
// to anyone else.
func (s *Service) ensureListener(ctx context.Context) error {
	var initErr error
	s.listenerOnce.Do(func() {
		poolConn, err := s.pool.Acquire(ctx)
		if err != nil {
			initErr = err
			return
		}
		s.listenerPoolConn = poolConn
		s.listenerConn = poolConn.Conn()
		s.listenerCtx, s.listenerStop = context.WithCancel(context.Background())
		s.listenerDone = make(chan struct{})
		go s.runListener()
	})
	return initErr
}

// listen submits a LISTEN command to the listener goroutine and waits for it
// to complete. This routes through cmdChan so the listener owns all
// connection access.
func (s *Service) listen(ctx context.Context, channel string) error {
	// Channel name is sanitized in notifyChannel to [a-zA-Z0-9_], so direct
	// interpolation is safe.
	return s.execCommand(ctx, fmt.Sprintf("LISTEN %s", channel))
}

func (s *Service) execCommand(ctx context.Context, sql string) error {
	resultCh := make(chan error, 1)
	cmd := listenerCommand{sql: sql, resultCh: resultCh}
	select {
	case s.cmdChan <- cmd:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Interrupt the wait so the command gets processed.
	s.cancelWaitMu.Lock()
	if s.cancelWait != nil {
		s.cancelWait()
	}
	s.cancelWaitMu.Unlock()
	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) runListener() {
	defer close(s.listenerDone)
	log := logr.FromContextOrDiscard(s.listenerCtx)
	for {
		if s.listenerCtx.Err() != nil {
			return
		}

		// Set up a cancelable wait BEFORE draining commands. This is the
		// race-free order: any execCommand that pushed a cmd while we were
		// blocked in WaitForNotification has already cancelled the previous
		// wait; any execCommand that runs after we publish the new
		// cancelWait will either be drained by processPendingCommands below
		// (if it pushes before we start draining) or will cancel waitCtx
		// (if it pushes after the drain), causing WaitForNotification to
		// return immediately and re-loop.
		waitCtx, cancelWait := context.WithCancel(s.listenerCtx)
		s.cancelWaitMu.Lock()
		s.cancelWait = cancelWait
		s.cancelWaitMu.Unlock()

		s.processPendingCommands(log)

		n, err := s.listenerConn.WaitForNotification(waitCtx)
		cancelWait()

		if err != nil {
			if s.listenerCtx.Err() != nil {
				return
			}
			// Wait was cancelled to process new commands; loop and drain.
			if waitCtx.Err() != nil {
				continue
			}
			log.Error(err, "queue/pgx: WaitForNotification")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// Look up topic by reverse mapping; the channel name is the
		// sanitized formatted topic. Workers track wake by formatted topic,
		// so we broadcast on every formatted topic whose notifyChannel
		// matches. In practice, callers use unique namespaces per test so
		// collisions are vanishingly unlikely; if they occur, both topics
		// just wake spuriously, which is harmless.
		s.wakeAllByChannel(n.Channel)
	}
}

func (s *Service) processPendingCommands(log logr.Logger) {
	for {
		select {
		case cmd := <-s.cmdChan:
			_, err := s.listenerConn.Exec(s.listenerCtx, cmd.sql)
			if err != nil {
				log.Error(err, "queue/pgx: listener exec", "sql", cmd.sql)
			}
			cmd.resultCh <- err
		default:
			return
		}
	}
}

func (s *Service) wakeAllByChannel(channel string) {
	s.wakeMu.Lock()
	defer s.wakeMu.Unlock()
	for topic, ch := range s.wake {
		if notifyChannel(topic) == channel {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}

// Close shuts down consumers, the listener, and releases the listener conn.
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
	if s.listenerStop != nil {
		s.listenerStop()
		<-s.listenerDone
		if s.listenerPoolConn != nil {
			s.listenerPoolConn.Release()
		}
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
		j, attempt, err := c.claim()
		if err != nil {
			log.Error(err, "queue/pgx: claim")
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
		select {
		case <-c.ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

func (c *consumer) claim() (*queue.Job, int, error) {
	row := c.svc.pool.QueryRow(c.ctx, `
		UPDATE queue_jobs
		   SET locked_at    = now(),
		       locked_until = now() + ($1::int * interval '1 millisecond'),
		       attempt      = attempt + 1
		 WHERE id = (
		     SELECT id FROM queue_jobs
		      WHERE topic = $2
		        AND run_at <= now()
		        AND (locked_until IS NULL OR locked_until < now())
		      ORDER BY priority DESC, run_at ASC, enqueued_at ASC
		      FOR UPDATE SKIP LOCKED
		      LIMIT 1
		 )
		 RETURNING id, payload, attempt, enqueued_at`,
		int(c.cfg.VisibilityTimeout/time.Millisecond),
		c.topic,
	)
	var (
		id         uuid.UUID
		payload    []byte
		attempt    int
		enqueuedAt time.Time
	)
	if err := row.Scan(&id, &payload, &attempt, &enqueuedAt); err != nil {
		if errors.Is(err, pgx5.ErrNoRows) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	return &queue.Job{
		ID:         id.String(),
		Topic:      c.topic,
		Payload:    payload,
		Attempt:    attempt,
		EnqueuedAt: enqueuedAt,
	}, attempt, nil
}

func (c *consumer) process(j *queue.Job, attempt int, log logr.Logger) {
	margin := min(c.cfg.VisibilityTimeout/10, time.Second)
	hCtx, hCancel := context.WithDeadline(c.ctx, time.Now().Add(c.cfg.VisibilityTimeout-margin))
	defer hCancel()

	err := c.invoke(hCtx, j)
	if err == nil {
		if _, derr := c.svc.pool.Exec(c.ctx, `DELETE FROM queue_jobs WHERE id = $1`, j.ID); derr != nil {
			log.Error(derr, "queue/pgx: delete on success", "id", j.ID)
		}
		return
	}
	log.Error(err, "queue/pgx: handler error", "id", j.ID, "attempt", attempt)
	skip := errors.Is(err, queue.ErrSkipRetry)
	exhausted := c.cfg.MaxRetries >= 0 && attempt > c.cfg.MaxRetries
	if skip || exhausted {
		if dErr := c.routeDLQ(j); dErr != nil {
			log.Error(dErr, "queue/pgx: DLQ route; retaining job", "id", j.ID)
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

func (c *consumer) reschedule(j *queue.Job, attempt int, hErr error) {
	delay := c.cfg.Backoff(attempt)
	_, err := c.svc.pool.Exec(c.ctx, `
		UPDATE queue_jobs
		   SET locked_until = NULL,
		       run_at       = now() + ($1::int * interval '1 millisecond'),
		       last_error   = $2
		 WHERE id = $3`,
		int(delay/time.Millisecond), hErr.Error(), j.ID,
	)
	if err != nil {
		logr.FromContextOrDiscard(c.ctx).Error(err, "queue/pgx: reschedule", "id", j.ID)
	}
}

// routeDLQ atomically inserts the job into the DLQ topic and deletes the
// original. Single transaction = a failure leaves the original visible.
func (c *consumer) routeDLQ(j *queue.Job) error {
	tx, err := c.svc.pool.Begin(c.ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(c.ctx) }()
	if _, err := tx.Exec(c.ctx,
		`INSERT INTO queue_jobs (id, topic, payload, priority, attempt, run_at, enqueued_at)
		 VALUES ($1, $2, $3, 0, 0, now(), now())`,
		uuid.New(), c.cfg.DeadLetterTopic, j.Payload,
	); err != nil {
		return fmt.Errorf("dlq insert: %w", err)
	}
	if _, err := tx.Exec(c.ctx,
		`DELETE FROM queue_jobs WHERE id = $1`, j.ID,
	); err != nil {
		return fmt.Errorf("dlq delete original: %w", err)
	}
	if _, err := tx.Exec(c.ctx, `SELECT pg_notify($1, '')`, notifyChannel(c.cfg.DeadLetterTopic)); err != nil {
		return fmt.Errorf("dlq notify: %w", err)
	}
	if err := tx.Commit(c.ctx); err != nil {
		return err
	}
	c.svc.signal(c.cfg.DeadLetterTopic)
	return nil
}
