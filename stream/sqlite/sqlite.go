package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/enverbisevac/libs/stream"
)

//go:embed schema.sql
var Schema string

// Apply executes the embedded schema against db. Idempotent.
func Apply(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, Schema)
	return err
}

// ErrClosed is returned by Publish or Subscribe after Close.
var ErrClosed = errors.New("stream/sqlite: service closed")

// Service is the SQLite stream.Service implementation.
type Service struct {
	cfg stream.ServiceConfig
	db  *sql.DB

	mu        sync.Mutex
	closed    bool
	consumers []*consumer

	wakeMu sync.Mutex
	wake   map[string]chan struct{}
}

// New constructs a stream/sqlite Service. The caller owns db.
func New(db *sql.DB, opts ...stream.ServiceOption) *Service {
	var cfg stream.ServiceConfig
	for _, o := range opts {
		o.Apply(&cfg)
	}
	stream.ResolveServiceConfig(&cfg)
	return &Service{
		cfg:  cfg,
		db:   db,
		wake: make(map[string]chan struct{}),
	}
}

// Close shuts down all consumers. Idempotent. ctx bounds the overall
// close: cancellation aborts before the next consumer is drained and
// returns ctx.Err() — already-drained consumers stay drained.
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

// Publish appends a message to the stream. After insert, signals any
// local subscribers via the per-stream wake channel and runs trim if
// MaxLen/MaxAge are set.
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

	hdrs, err := encodeHeaders(pcfg.Headers)
	if err != nil {
		return fmt.Errorf("stream/sqlite: encode headers: %w", err)
	}

	now := time.Now().UnixNano()
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO stream_messages (stream, payload, headers, created_at) VALUES (?, ?, ?, ?)`,
		formatted, payload, hdrs, now,
	); err != nil {
		return fmt.Errorf("stream/sqlite: insert: %w", err)
	}

	s.signal(formatted)

	if pcfg.MaxLen > 0 || pcfg.MaxAge > 0 {
		// Trim is best-effort; the message is already published and
		// signal() has already woken subscribers. A trim failure here
		// will be silently retried on the next publish.
		_ = s.applyTrim(ctx, formatted, pcfg.MaxLen, pcfg.MaxAge, now)
	}
	return nil
}

// applyTrim deletes rows past MaxLen (oldest first) and rows older than
// MaxAge. Either bound being zero means "skip that branch". When both
// bounds are set, the length cap is applied first, then the age cap.
func (s *Service) applyTrim(ctx context.Context, streamName string, maxLen int64, maxAge time.Duration, nowNanos int64) error {
	if maxLen > 0 {
		if _, err := s.db.ExecContext(ctx, `
			DELETE FROM stream_messages
			 WHERE stream = ?
			   AND id IN (
			       SELECT id FROM stream_messages
			        WHERE stream = ?
			        ORDER BY id ASC
			        LIMIT MAX(0, (SELECT COUNT(*) FROM stream_messages WHERE stream = ?) - ?)
			   )`,
			streamName, streamName, streamName, maxLen,
		); err != nil {
			return fmt.Errorf("trim by maxlen: %w", err)
		}
	}
	if maxAge > 0 {
		cutoff := nowNanos - maxAge.Nanoseconds()
		if _, err := s.db.ExecContext(ctx,
			`DELETE FROM stream_messages WHERE stream = ? AND created_at < ?`,
			streamName, cutoff,
		); err != nil {
			return fmt.Errorf("trim by maxage: %w", err)
		}
	}
	return nil
}

// Subscribe lazily creates the consumer group at the requested start
// position and spawns concurrency-many workers.
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
		s.signal(c.stream) // wake any worker parked on the channel
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

// ensureGroup inserts the (stream, group) row at the resolved start_seq if
// it doesn't already exist. ON CONFLICT DO NOTHING — the durable cursor
// wins for existing groups.
func (s *Service) ensureGroup(ctx context.Context, streamName, group string, scfg stream.SubscribeConfig) error {
	startSeq, err := s.resolveStartSeq(ctx, streamName, scfg)
	if err != nil {
		return err
	}
	now := time.Now().UnixNano()
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO stream_groups (stream, group_name, last_id, created_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT (stream, group_name) DO NOTHING`,
		streamName, group, startSeq, now,
	)
	return err
}

// resolveStartSeq computes the seq value a brand-new group should start at.
func (s *Service) resolveStartSeq(ctx context.Context, streamName string, scfg stream.SubscribeConfig) (int64, error) {
	switch {
	case scfg.StartFromID != "":
		if seq, err := strconv.ParseInt(scfg.StartFromID, 10, 64); err == nil {
			return seq, nil
		}
		return s.maxSeq(ctx, streamName)
	case scfg.StartFrom == stream.StartEarliest:
		return 0, nil
	default: // StartLatest
		return s.maxSeq(ctx, streamName)
	}
}

func (s *Service) maxSeq(ctx context.Context, streamName string) (int64, error) {
	var seq sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT MAX(id) FROM stream_messages WHERE stream = ?`, streamName,
	).Scan(&seq)
	if err != nil {
		return 0, err
	}
	if !seq.Valid {
		return 0, nil
	}
	return seq.Int64, nil
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

// wakeChan returns the close-and-replace wake channel for a stream.
// Workers select on this channel; publishers close it (under wakeMu) to
// wake all selectors and replace it with a fresh open channel for the
// next cycle. Mirrors stream/inmem's notifyCh pattern.
func (s *Service) wakeChan(stream string) chan struct{} {
	s.wakeMu.Lock()
	defer s.wakeMu.Unlock()
	ch, ok := s.wake[stream]
	if !ok {
		ch = make(chan struct{})
		s.wake[stream] = ch
	}
	return ch
}

// signal closes the current wake channel for a stream (waking all
// listeners) and replaces it with a fresh open channel.
func (s *Service) signal(stream string) {
	s.wakeMu.Lock()
	defer s.wakeMu.Unlock()
	if ch, ok := s.wake[stream]; ok {
		close(ch)
		s.wake[stream] = make(chan struct{})
	}
}

// encodeHeaders encodes a header map as a compact JSON object. nil/empty
// returns "{}".
func encodeHeaders(h map[string]string) (string, error) {
	if len(h) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(h)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodeHeaders parses a JSON object into a header map. Empty/"{}" returns nil.
func decodeHeaders(s string) (map[string]string, error) {
	if s == "" || s == "{}" {
		return nil, nil
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// hasWork is a read-only pre-check: returns true if there are reclaimable
// pending rows for this group OR new messages past the group's cursor.
// Doesn't acquire the write lock, so idle polling doesn't contend with
// publishers.
func (c *consumer) hasWork(nowNs int64) (bool, error) {
	var found int
	err := c.svc.db.QueryRowContext(c.ctx, `
		SELECT 1 WHERE EXISTS (
			SELECT 1 FROM stream_pending
			 WHERE stream = ? AND group_name = ? AND claim_deadline < ?
		) OR EXISTS (
			SELECT 1 FROM stream_messages
			 WHERE stream = ? AND id > (
			       SELECT last_id FROM stream_groups
			        WHERE stream = ? AND group_name = ?
			   )
		)`,
		c.stream, c.group, nowNs,
		c.stream, c.stream, c.group,
	).Scan(&found)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("hasWork: %w", err)
	}
	return true, nil
}

// pendingRow is one in-flight claim returned by claim().
type pendingRow struct {
	msgID       int64
	payload     []byte
	headersJSON string
	createdAtNs int64
	attempts    int
	consumer    string
}

// worker is the per-goroutine dispatch loop. It claims new entries past
// the group's cursor (with reclaim of expired pending entries first),
// runs the handler, and acks/DLQs in runOne.
func (c *consumer) worker(consumerName string) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()

	for {
		if c.ctx.Err() != nil {
			return
		}

		batch, err := c.claim(consumerName, time.Now())
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

		// Idle wait. Re-fetch the wake channel each cycle because
		// signal() replaces it after each broadcast.
		wake := c.svc.wakeChan(c.stream)
		select {
		case <-c.ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

// claim runs the BEGIN IMMEDIATE → reclaim-expired → claim-new pattern.
// Returns the batch of rows to dispatch (or nil if nothing is ready).
//
// Uses BEGIN IMMEDIATE so concurrent workers serialize on the write lock at
// transaction start instead of racing to upgrade from a read snapshot — the
// latter produces SQLITE_BUSY (or stale-snapshot reads) when two workers
// both SELECT pending rows then both try to UPDATE.
//
// A cheap pre-check (read-only, doesn't touch the write lock) skips the
// IMMEDIATE entirely when there is nothing to do — empty polls would
// otherwise serialize all workers and starve publishers.
func (c *consumer) claim(consumerName string, now time.Time) ([]pendingRow, error) {
	nowNs := now.UnixNano()

	if hasWork, err := c.hasWork(nowNs); err != nil {
		return nil, err
	} else if !hasWork {
		return nil, nil
	}

	conn, err := c.svc.db.Conn(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("conn: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(c.ctx, "BEGIN IMMEDIATE"); err != nil {
		return nil, fmt.Errorf("begin immediate: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	const batchSize = 16
	out := make([]pendingRow, 0, batchSize)
	deadlineNs := now.Add(c.cfg.VisibilityTimeout).UnixNano()

	// 1. Reclaim expired pending rows. UPDATE...RETURNING gives the
	// (msg_id, attempts) pairs of the rows we successfully reclaimed.
	expRows, err := conn.QueryContext(c.ctx, `
		UPDATE stream_pending
		   SET consumer = ?, claim_deadline = ?, attempts = attempts + 1
		 WHERE stream = ? AND group_name = ?
		   AND claim_deadline < ?
		   AND msg_id IN (
		       SELECT msg_id FROM stream_pending
		        WHERE stream = ? AND group_name = ? AND claim_deadline < ?
		        ORDER BY claim_deadline ASC
		        LIMIT ?
		   )
		 RETURNING msg_id, attempts`,
		consumerName, deadlineNs, c.stream, c.group, nowNs,
		c.stream, c.group, nowNs, batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("reclaim: %w", err)
	}
	type expEntry struct {
		msgID    int64
		attempts int
	}
	var reclaimed []expEntry
	for expRows.Next() {
		var e expEntry
		if err := expRows.Scan(&e.msgID, &e.attempts); err != nil {
			_ = expRows.Close()
			return nil, fmt.Errorf("reclaim scan: %w", err)
		}
		reclaimed = append(reclaimed, e)
	}
	_ = expRows.Close()
	if err := expRows.Err(); err != nil {
		return nil, fmt.Errorf("reclaim rows err: %w", err)
	}

	// Fetch the message bodies for the reclaimed IDs.
	for _, e := range reclaimed {
		var pr pendingRow
		pr.msgID = e.msgID
		pr.attempts = e.attempts
		pr.consumer = consumerName
		if err := conn.QueryRowContext(c.ctx,
			`SELECT payload, headers, created_at FROM stream_messages WHERE id = ?`,
			e.msgID,
		).Scan(&pr.payload, &pr.headersJSON, &pr.createdAtNs); err != nil {
			return nil, fmt.Errorf("reclaim body: %w", err)
		}
		out = append(out, pr)
		if len(out) >= batchSize {
			break
		}
	}

	// 2. Claim new entries past the cursor up to the remaining capacity.
	remaining := batchSize - len(out)
	if remaining > 0 {
		var lastID int64
		if err := conn.QueryRowContext(c.ctx,
			`SELECT last_id FROM stream_groups WHERE stream = ? AND group_name = ?`,
			c.stream, c.group,
		).Scan(&lastID); err != nil {
			return nil, fmt.Errorf("read cursor: %w", err)
		}

		newRows, err := conn.QueryContext(c.ctx, `
			SELECT id, payload, headers, created_at
			  FROM stream_messages
			 WHERE stream = ? AND id > ?
			 ORDER BY id ASC
			 LIMIT ?`,
			c.stream, lastID, remaining,
		)
		if err != nil {
			return nil, fmt.Errorf("query new: %w", err)
		}
		newRowsList := make([]pendingRow, 0, remaining)
		for newRows.Next() {
			var pr pendingRow
			if err := newRows.Scan(&pr.msgID, &pr.payload, &pr.headersJSON, &pr.createdAtNs); err != nil {
				_ = newRows.Close()
				return nil, fmt.Errorf("scan new: %w", err)
			}
			pr.attempts = 1
			pr.consumer = consumerName
			newRowsList = append(newRowsList, pr)
		}
		_ = newRows.Close()
		if err := newRows.Err(); err != nil {
			return nil, fmt.Errorf("new rows err: %w", err)
		}

		for _, pr := range newRowsList {
			if _, err := conn.ExecContext(c.ctx,
				`INSERT INTO stream_pending (stream, group_name, msg_id, consumer, claim_deadline, attempts)
				 VALUES (?, ?, ?, ?, ?, 1)`,
				c.stream, c.group, pr.msgID, consumerName, deadlineNs,
			); err != nil {
				return nil, fmt.Errorf("insert pending: %w", err)
			}
		}

		if len(newRowsList) > 0 {
			maxNew := newRowsList[len(newRowsList)-1].msgID
			if _, err := conn.ExecContext(c.ctx,
				`UPDATE stream_groups SET last_id = ? WHERE stream = ? AND group_name = ?`,
				maxNew, c.stream, c.group,
			); err != nil {
				return nil, fmt.Errorf("advance cursor: %w", err)
			}
		}

		out = append(out, newRowsList...)
	}

	if _, err := conn.ExecContext(c.ctx, "COMMIT"); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	committed = true
	return out, nil
}

// runOne builds a Message from the pending row, runs the handler, and
// either acks (delete pending), routes to DLQ (atomic insert+delete in
// one tx), or leaves the row pending for the reclaim path.
func (c *consumer) runOne(p pendingRow) {
	hdrs, _ := decodeHeaders(p.headersJSON)
	msg := &stream.Message{
		ID:        strconv.FormatInt(p.msgID, 10),
		Stream:    c.stream,
		Payload:   p.payload,
		Headers:   hdrs,
		CreatedAt: time.Unix(0, p.createdAtNs),
		Attempt:   p.attempts,
	}

	err := safeRun(c.ctx, c.handler, msg)

	if err == nil {
		// Identity-check: only delete if the pending row's consumer
		// column still matches us. If a parallel reclaim has taken the
		// row, its claim is in-flight and must not be cleared by us.
		_, _ = c.svc.db.ExecContext(c.ctx,
			`DELETE FROM stream_pending
			  WHERE stream = ? AND group_name = ? AND msg_id = ? AND consumer = ?`,
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

// safeRun runs the handler with panic recovery. Panics become errors
// with a stack trace.
func safeRun(ctx context.Context, h stream.Handler, msg *stream.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("stream/sqlite: handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, msg)
}

// routeDLQ atomically inserts the message into the DLQ stream and
// deletes the pending row. Mirrors stream/inmem's publishToDLQ but in a
// single transaction since SQLite makes it cheap.
//
// Append-before-delete is intentional: at-least-once. If the DLQ insert
// or commit fails, the pending row stays put and reclaim will retry.
//
// BEGIN IMMEDIATE matches the claim() pattern — both contend for the
// same write lock; deferred would risk SQLITE_BUSY on upgrade.
func (c *consumer) routeDLQ(p pendingRow) {
	conn, err := c.svc.db.Conn(c.ctx)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(c.ctx, "BEGIN IMMEDIATE"); err != nil {
		return
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	now := time.Now().UnixNano()
	if _, err := conn.ExecContext(c.ctx,
		`INSERT INTO stream_messages (stream, payload, headers, created_at) VALUES (?, ?, ?, ?)`,
		c.cfg.DeadLetterStream, p.payload, p.headersJSON, now,
	); err != nil {
		return
	}
	if _, err := conn.ExecContext(c.ctx,
		`DELETE FROM stream_pending
		  WHERE stream = ? AND group_name = ? AND msg_id = ? AND consumer = ?`,
		c.stream, c.group, p.msgID, p.consumer,
	); err != nil {
		return
	}
	if _, err := conn.ExecContext(c.ctx, "COMMIT"); err != nil {
		return
	}
	committed = true
	c.svc.signal(c.cfg.DeadLetterStream)
}
