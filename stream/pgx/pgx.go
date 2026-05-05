package pgx

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	pgx5 "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/enverbisevac/libs/stream"
)

//go:embed schema.sql
var Schema string

// Apply executes the embedded schema against pool. Idempotent.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, Schema)
	return err
}

// ErrClosed is returned by Publish or Subscribe after Close.
var ErrClosed = errors.New("stream/pgx: service closed")

// listenerCommand is queued onto Service.cmdChan for the listener
// goroutine to execute on its dedicated connection. This serializes
// Exec calls with WaitForNotification, since pgx.Conn is not safe for
// concurrent use.
type listenerCommand struct {
	sql      string
	resultCh chan error
}

// Service is the pgx stream.Service implementation.
type Service struct {
	cfg  stream.ServiceConfig
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

	// cancelWaitMu protects cancelWait. The listener loop sets this
	// each time it enters WaitForNotification; execCommand cancels it
	// to wake the listener.
	cancelWaitMu sync.Mutex
	cancelWait   context.CancelFunc

	// Per-stream broadcast channels for wake-up. Buffered cap=1; sends
	// are non-blocking.
	wakeMu sync.Mutex
	wake   map[string]chan struct{}
}

// New constructs a stream/pgx Service. The caller owns pool.
func New(pool *pgxpool.Pool, opts ...stream.ServiceOption) *Service {
	var cfg stream.ServiceConfig
	for _, o := range opts {
		o.Apply(&cfg)
	}
	stream.ResolveServiceConfig(&cfg)
	return &Service{
		cfg:     cfg,
		pool:    pool,
		wake:    make(map[string]chan struct{}),
		cmdChan: make(chan listenerCommand, 16),
	}
}

func (s *Service) wakeChan(streamName string) chan struct{} {
	s.wakeMu.Lock()
	defer s.wakeMu.Unlock()
	ch, ok := s.wake[streamName]
	if !ok {
		ch = make(chan struct{}, 1)
		s.wake[streamName] = ch
	}
	return ch
}

func (s *Service) signal(streamName string) {
	ch := s.wakeChan(streamName)
	select {
	case ch <- struct{}{}:
	default:
	}
}

// notifyChannel maps a formatted stream name to a Postgres LISTEN
// channel name. PG identifiers cap at 63 chars; we sanitize to
// [a-zA-Z0-9_] and truncate.
func notifyChannel(formattedStream string) string {
	const prefix = "stream_"
	var b strings.Builder
	b.Grow(len(prefix) + len(formattedStream))
	b.WriteString(prefix)
	for _, r := range formattedStream {
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

// Publish appends a message and emits a NOTIFY for cross-process
// wake-ups, then signals local subscribers and runs trim if MaxLen/
// MaxAge are set.
func (s *Service) Publish(ctx context.Context, name string, payload []byte, opts ...stream.PublishOption) error {
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

	headersJSON, err := json.Marshal(headersOrEmpty(pcfg.Headers))
	if err != nil {
		return fmt.Errorf("stream/pgx: encode headers: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("stream/pgx: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Per-stream advisory lock serializes commits, eliminating the
	// "uncommitted-id gap" race: under READ COMMITTED, a BIGSERIAL id
	// allocated by an in-flight publisher is invisible to claim until
	// the publisher commits. If two publishers race and the higher-id
	// commits first, a claiming worker would advance its cursor past
	// the lower id (still uncommitted) and never deliver it. Holding
	// pg_advisory_xact_lock(hashtext(stream)) until commit forces
	// publishes on the same stream to commit in id-allocation order.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, formatted); err != nil {
		return fmt.Errorf("stream/pgx: advisory lock: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO stream_messages (stream, payload, headers) VALUES ($1, $2, $3::jsonb)`,
		formatted, payload, string(headersJSON),
	); err != nil {
		return fmt.Errorf("stream/pgx: insert: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT pg_notify($1, '')`, notifyChannel(formatted),
	); err != nil {
		return fmt.Errorf("stream/pgx: notify: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("stream/pgx: commit: %w", err)
	}

	s.signal(formatted)

	if pcfg.MaxLen > 0 || pcfg.MaxAge > 0 {
		// Trim is best-effort post-commit; the message is already
		// published. Errors are silently swallowed (will retry on next
		// publish).
		_ = s.applyTrim(ctx, formatted, pcfg.MaxLen, pcfg.MaxAge)
	}
	return nil
}

func headersOrEmpty(h map[string]string) map[string]string {
	if h == nil {
		return map[string]string{}
	}
	return h
}

// applyTrim deletes rows past MaxLen (oldest first) and rows older than
// MaxAge. Either bound being zero means "skip that branch". When both
// bounds are set, the length cap is applied first, then the age cap.
func (s *Service) applyTrim(ctx context.Context, streamName string, maxLen int64, maxAge time.Duration) error {
	if maxLen > 0 {
		// Keep the most recent maxLen rows. ORDER BY id DESC OFFSET N
		// returns IDs older than the N-th newest — those are what we
		// delete.
		if _, err := s.pool.Exec(ctx, `
			DELETE FROM stream_messages
			 WHERE stream = $1
			   AND id IN (
			       SELECT id FROM stream_messages
			        WHERE stream = $1
			        ORDER BY id DESC
			        OFFSET $2
			   )`,
			streamName, maxLen,
		); err != nil {
			return fmt.Errorf("trim by maxlen: %w", err)
		}
	}
	if maxAge > 0 {
		cutoff := time.Now().Add(-maxAge)
		if _, err := s.pool.Exec(ctx,
			`DELETE FROM stream_messages WHERE stream = $1 AND created_at < $2`,
			streamName, cutoff,
		); err != nil {
			return fmt.Errorf("trim by maxage: %w", err)
		}
	}
	return nil
}

// Subscribe lazily creates the consumer group at the requested start
// position, ensures the LISTEN connection is up, and spawns
// concurrency-many workers.
func (s *Service) Subscribe(ctx context.Context, name, group string, h stream.Handler, opts ...stream.SubscribeOption) (stream.Consumer, error) {
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

	if err := s.ensureGroup(ctx, formatted, group, scfg); err != nil {
		return nil, err
	}
	if err := s.ensureListener(ctx); err != nil {
		return nil, err
	}
	if err := s.listen(ctx, notifyChannel(formatted)); err != nil {
		return nil, err
	}

	// Worker ctx is independent of the caller's ctx so that canceling the
	// request that called Subscribe does not stop the consumer. Lifetime
	// is controlled exclusively by Consumer.Close (via cancel below).
	cctx, cancel := context.WithCancel(context.Background())
	c := &consumer{
		svc:     s,
		cfg:     scfg,
		stream:  formatted,
		group:   group,
		handler: h,
		ctx:     cctx,
		cancel:  cancel,
	}
	c.wg.Add(scfg.Concurrency)
	for i := range scfg.Concurrency {
		consumerName := fmt.Sprintf("%s-%d", group, i)
		go c.worker(consumerName)
	}

	c.closeFn = func() error {
		c.cancel()
		s.signal(c.stream)
		drained := make(chan struct{})
		go func() { c.wg.Wait(); close(drained) }()
		select {
		case <-drained:
		case <-time.After(c.cfg.ShutdownTimeout):
		}
		return nil
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

// ensureGroup inserts the (stream, group) row at the resolved start_seq
// if it doesn't already exist. ON CONFLICT DO NOTHING — durable cursor
// wins for existing groups.
func (s *Service) ensureGroup(ctx context.Context, streamName, group string, scfg stream.SubscribeConfig) error {
	startSeq, err := s.resolveStartSeq(ctx, streamName, scfg)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO stream_groups (stream, group_name, last_id)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (stream, group_name) DO NOTHING`,
		streamName, group, startSeq,
	)
	return err
}

func (s *Service) resolveStartSeq(ctx context.Context, streamName string, scfg stream.SubscribeConfig) (int64, error) {
	switch {
	case scfg.StartFromID != "":
		if seq, err := strconv.ParseInt(scfg.StartFromID, 10, 64); err == nil {
			return seq, nil
		}
		return s.maxSeq(ctx, streamName)
	case scfg.StartFrom == stream.StartEarliest:
		return 0, nil
	default:
		return s.maxSeq(ctx, streamName)
	}
}

func (s *Service) maxSeq(ctx context.Context, streamName string) (int64, error) {
	var seq sql.NullInt64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(id), 0) FROM stream_messages WHERE stream = $1`, streamName,
	).Scan(&seq)
	if err != nil {
		return 0, err
	}
	return seq.Int64, nil
}

type pendingRow struct {
	msgID       int64
	payload     []byte
	headersJSON []byte
	createdAt   time.Time
	attempts    int
	consumer    string
}

func (c *consumer) worker(consumerName string) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()

	for {
		if c.ctx.Err() != nil {
			return
		}

		batch, err := c.claim(consumerName)
		if err != nil {
			// Backoff on transient DB errors so we don't spin.
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if len(batch) > 0 {
			for _, p := range batch {
				c.runOne(p)
			}
			continue
		}

		wake := c.svc.wakeChan(c.stream)
		select {
		case <-c.ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

// claim runs reclaim-then-claim-new in two CTE round trips.
// Postgres' UPDATE...RETURNING + FOR UPDATE SKIP LOCKED makes both
// phases atomic and concurrent-safe across processes.
func (c *consumer) claim(consumerName string) ([]pendingRow, error) {
	const batchSize = 16
	out := make([]pendingRow, 0, batchSize)

	// 1. Reclaim expired pending rows.
	expRows, err := c.svc.pool.Query(c.ctx, `
		WITH reclaimed AS (
		    UPDATE stream_pending p
		       SET consumer = $1,
		           claim_deadline = now() + ($2::int * interval '1 millisecond'),
		           attempts = attempts + 1
		     WHERE (p.stream, p.group_name, p.msg_id) IN (
		         SELECT stream, group_name, msg_id
		           FROM stream_pending
		          WHERE stream = $3 AND group_name = $4 AND claim_deadline < now()
		          ORDER BY claim_deadline ASC
		          FOR UPDATE SKIP LOCKED
		          LIMIT $5
		     )
		     RETURNING msg_id, attempts
		)
		SELECT r.msg_id, m.payload, m.headers, m.created_at, r.attempts
		  FROM reclaimed r
		  JOIN stream_messages m ON m.id = r.msg_id`,
		consumerName, int(c.cfg.VisibilityTimeout/time.Millisecond),
		c.stream, c.group, batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("reclaim: %w", err)
	}
	for expRows.Next() {
		var pr pendingRow
		if err := expRows.Scan(&pr.msgID, &pr.payload, &pr.headersJSON, &pr.createdAt, &pr.attempts); err != nil {
			expRows.Close()
			return nil, fmt.Errorf("reclaim scan: %w", err)
		}
		pr.consumer = consumerName
		out = append(out, pr)
	}
	expRows.Close()
	if err := expRows.Err(); err != nil {
		return nil, fmt.Errorf("reclaim rows: %w", err)
	}

	// 2. Claim new entries past the cursor up to remaining batch size.
	remaining := batchSize - len(out)
	if remaining <= 0 {
		return out, nil
	}

	newRows, err := c.svc.pool.Query(c.ctx, `
		WITH cursor AS (
		    SELECT last_id FROM stream_groups
		     WHERE stream = $1 AND group_name = $2
		     FOR UPDATE
		), candidates AS (
		    -- No FOR UPDATE on m: each group has its own cursor row,
		    -- which is FOR UPDATE'd above to serialize claims within
		    -- this group. Row-level locks on stream_messages would
		    -- leak across groups: group B's concurrent claim would
		    -- SKIP LOCKED rows that group A's claim has locked,
		    -- jumping B's cursor past those messages and never
		    -- delivering them to B.
		    SELECT m.id, m.payload, m.headers, m.created_at
		      FROM stream_messages m, cursor c
		     WHERE m.stream = $1 AND m.id > c.last_id
		     ORDER BY m.id ASC
		     LIMIT $3
		), inserted AS (
		    INSERT INTO stream_pending (stream, group_name, msg_id, consumer, claim_deadline, attempts)
		    SELECT $1, $2, c.id, $4, now() + ($5::int * interval '1 millisecond'), 1
		      FROM candidates c
		    RETURNING msg_id
		), advance AS (
		    UPDATE stream_groups
		       SET last_id = (SELECT COALESCE(MAX(msg_id), (SELECT last_id FROM cursor)) FROM inserted)
		     WHERE stream = $1 AND group_name = $2
		)
		SELECT c.id, c.payload, c.headers, c.created_at FROM candidates c ORDER BY c.id ASC`,
		c.stream, c.group, remaining,
		consumerName, int(c.cfg.VisibilityTimeout/time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("claim new: %w", err)
	}
	for newRows.Next() {
		var pr pendingRow
		if err := newRows.Scan(&pr.msgID, &pr.payload, &pr.headersJSON, &pr.createdAt); err != nil {
			newRows.Close()
			return nil, fmt.Errorf("claim new scan: %w", err)
		}
		pr.attempts = 1
		pr.consumer = consumerName
		out = append(out, pr)
	}
	newRows.Close()
	if err := newRows.Err(); err != nil {
		return nil, fmt.Errorf("claim new rows: %w", err)
	}

	return out, nil
}

// runOne builds a Message from the pending row, runs the handler, and
// either acks (identity-checked delete from pending), routes to DLQ
// (atomic insert + delete in one tx), or leaves the row pending for
// the reclaim path.
func (c *consumer) runOne(p pendingRow) {
	hdrs, _ := decodeHeadersJSON(p.headersJSON)
	msg := &stream.Message{
		ID:        strconv.FormatInt(p.msgID, 10),
		Stream:    c.stream,
		Payload:   p.payload,
		Headers:   hdrs,
		CreatedAt: p.createdAt,
		Attempt:   p.attempts,
	}

	err := safeRun(c.ctx, c.handler, msg)

	if err == nil {
		// Identity-check: only delete if the pending row's consumer
		// column still matches us. If a parallel reclaim has taken the
		// row, its claim is in-flight and must not be cleared by us.
		_, _ = c.svc.pool.Exec(c.ctx,
			`DELETE FROM stream_pending
			  WHERE stream = $1 AND group_name = $2 AND msg_id = $3 AND consumer = $4`,
			c.stream, c.group, p.msgID, p.consumer,
		)
		return
	}

	skipRetry := errors.Is(err, stream.ErrSkipRetry)
	overBudget := c.cfg.MaxRetries >= 0 && p.attempts > c.cfg.MaxRetries

	if skipRetry || overBudget {
		c.routeDLQ(p)
		return
	}
	// Otherwise: leave the pending row alone. The reclaim path will
	// pick it up after claim_deadline elapses.
}

func decodeHeadersJSON(b []byte) (map[string]string, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var out map[string]string
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func safeRun(ctx context.Context, h stream.Handler, msg *stream.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("stream/pgx: handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, msg)
}

// routeDLQ atomically inserts the message into the DLQ stream, deletes
// the pending row (identity-checked), and emits a NOTIFY for the DLQ
// stream. All in one tx. Append-before-delete preserves at-least-once
// on tx failure.
func (c *consumer) routeDLQ(p pendingRow) {
	tx, err := c.svc.pool.Begin(c.ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(c.ctx) }()

	if _, err := tx.Exec(c.ctx,
		`INSERT INTO stream_messages (stream, payload, headers) VALUES ($1, $2, $3::jsonb)`,
		c.cfg.DeadLetterStream, p.payload, string(p.headersJSON),
	); err != nil {
		return
	}
	if _, err := tx.Exec(c.ctx,
		`DELETE FROM stream_pending
		  WHERE stream = $1 AND group_name = $2 AND msg_id = $3 AND consumer = $4`,
		c.stream, c.group, p.msgID, p.consumer,
	); err != nil {
		return
	}
	if _, err := tx.Exec(c.ctx, `SELECT pg_notify($1, '')`, notifyChannel(c.cfg.DeadLetterStream)); err != nil {
		return
	}
	if err := tx.Commit(c.ctx); err != nil {
		return
	}
	c.svc.signal(c.cfg.DeadLetterStream)
}

// Close shuts down consumers, stops the listener, and releases the
// listener's pool connection. Idempotent. ctx bounds the consumer-drain
// phase: cancellation aborts before the next consumer is drained and
// returns ctx.Err(). The listener is always stopped before returning so
// the pool connection isn't leaked.
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
	var ctxErr error
	for _, c := range consumers {
		if err := ctx.Err(); err != nil {
			ctxErr = err
			break
		}
		_ = c.Close()
	}
	if s.listenerStop != nil {
		s.listenerStop()
		<-s.listenerDone
		if s.listenerPoolConn != nil {
			s.listenerPoolConn.Release()
		}
	}
	return ctxErr
}

type consumer struct {
	svc     *Service
	cfg     stream.SubscribeConfig
	stream  string
	group   string
	handler stream.Handler

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
	closeFn   func() error
}

func (c *consumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		if c.closeFn != nil {
			err = c.closeFn()
		}
	})
	return err
}

var _ stream.Service = (*Service)(nil)

// ensureListener acquires a dedicated pool connection and starts the
// listener goroutine on first call. We hold the pgxpool.Conn
// indefinitely so the underlying conn isn't returned to the pool.
func (s *Service) ensureListener(ctx context.Context) error {
	var initErr error
	s.listenerOnce.Do(func() {
		poolConn, err := s.pool.Acquire(ctx)
		if err != nil {
			initErr = err
			return
		}
		// Clear any LISTEN registrations leftover from a previous owner
		// of this pool connection. pgxpool reuses connections, and
		// LISTEN state is per-connection, not per-Service. Without this,
		// a fresh Service inherits stale subscriptions and gets spurious
		// wake-ups (and over time, bloats listener wake-up handling).
		if _, err := poolConn.Conn().Exec(ctx, "UNLISTEN *"); err != nil {
			poolConn.Release()
			initErr = fmt.Errorf("stream/pgx: UNLISTEN *: %w", err)
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

// listen submits a LISTEN command to the listener goroutine and waits
// for it to complete.
func (s *Service) listen(ctx context.Context, channel string) error {
	return s.execCommand(ctx, fmt.Sprintf("LISTEN %s", channel))
}

func (s *Service) execCommand(ctx context.Context, sqlStmt string) error {
	resultCh := make(chan error, 1)
	cmd := listenerCommand{sql: sqlStmt, resultCh: resultCh}
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
	for {
		if s.listenerCtx.Err() != nil {
			return
		}

		// Set up cancelable wait BEFORE draining commands. Race-free order:
		// any execCommand that pushed a cmd while we were blocked has
		// already cancelled the previous wait; any execCommand after we
		// publish the new cancelWait will either be drained by
		// processPendingCommands (if it pushes before we start draining)
		// or will cancel waitCtx (if it pushes after the drain), causing
		// WaitForNotification to return immediately.
		waitCtx, cancelWait := context.WithCancel(s.listenerCtx)
		s.cancelWaitMu.Lock()
		s.cancelWait = cancelWait
		s.cancelWaitMu.Unlock()

		s.processPendingCommands()

		n, err := s.listenerConn.WaitForNotification(waitCtx)
		cancelWait()

		if err != nil {
			if s.listenerCtx.Err() != nil {
				return
			}
			if waitCtx.Err() != nil {
				continue
			}
			// Transient error; small backoff to avoid spinning.
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s.wakeAllByChannel(n.Channel)
	}
}

func (s *Service) processPendingCommands() {
	for {
		select {
		case cmd := <-s.cmdChan:
			_, err := s.listenerConn.Exec(s.listenerCtx, cmd.sql)
			cmd.resultCh <- err
		default:
			return
		}
	}
}

func (s *Service) wakeAllByChannel(channel string) {
	s.wakeMu.Lock()
	defer s.wakeMu.Unlock()
	for streamName, ch := range s.wake {
		if notifyChannel(streamName) == channel {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}
