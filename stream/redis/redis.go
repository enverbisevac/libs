package redis

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	rds "github.com/redis/go-redis/v9"

	"github.com/enverbisevac/libs/stream"
)

// ErrClosed is returned by Publish or Subscribe after Close.
var ErrClosed = errors.New("stream/redis: service closed")

// Service is the Redis Streams stream.Service implementation.
type Service struct {
	cfg    stream.ServiceConfig
	client rds.UniversalClient

	mu        sync.Mutex
	closed    bool
	consumers []*consumer
}

// New constructs a stream/redis Service. The caller owns client.
func New(client rds.UniversalClient, opts ...stream.ServiceOption) *Service {
	var cfg stream.ServiceConfig
	for _, o := range opts {
		o.Apply(&cfg)
	}
	stream.ResolveServiceConfig(&cfg)
	return &Service{
		cfg:    cfg,
		client: client,
	}
}

// streamKey returns the Redis key used for a formatted stream name. We
// use a "stream:" prefix to keep the key namespace distinct from
// other Redis usage.
func streamKey(formatted string) string {
	return "stream:" + formatted
}

// Close drains all consumers. Idempotent. ctx bounds the overall close:
// cancellation aborts before the next consumer is drained and returns
// ctx.Err() — already-drained consumers stay drained.
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

// Publish sends a message via XADD. The payload is stored under the
// "payload" field; headers are stored as additional fields with "h:"
// prefix. MaxLen is applied as XADD MAXLEN ~ (approximate trimming for
// speed). MaxAge is applied via XTRIM MINID after the publish.
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

	values := make([]any, 0, 2+2*len(pcfg.Headers))
	values = append(values, "payload", payload)
	for k, v := range pcfg.Headers {
		values = append(values, "h:"+k, v)
	}

	args := &rds.XAddArgs{
		Stream: streamKey(formatted),
		ID:     "*",
		Values: values,
	}
	if pcfg.MaxLen > 0 {
		// Exact MAXLEN (not Approx ~) so the conformance contract
		// "at most N entries after publish" holds. Approx trimming
		// can leave more than N entries to avoid memory rebalance.
		args.MaxLen = pcfg.MaxLen
	}

	if _, err := s.client.XAdd(ctx, args).Result(); err != nil {
		return fmt.Errorf("stream/redis: xadd: %w", err)
	}

	// MaxAge trim is best-effort; piggyback on each publish.
	if pcfg.MaxAge > 0 {
		cutoffMs := time.Now().Add(-pcfg.MaxAge).UnixMilli()
		minID := fmt.Sprintf("%d-0", cutoffMs)
		_ = s.client.XTrimMinID(ctx, streamKey(formatted), minID).Err()
	}
	return nil
}

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

	startID := resolveStartID(scfg)

	// XGROUP CREATE with MKSTREAM. If the group already exists, Redis
	// returns "BUSYGROUP" — we catch and ignore (durable cursor wins).
	if err := s.client.XGroupCreateMkStream(ctx, streamKey(formatted), group, startID).Err(); err != nil {
		if !strings.HasPrefix(err.Error(), "BUSYGROUP") {
			return nil, fmt.Errorf("stream/redis: xgroup create: %w", err)
		}
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

// resolveStartID returns the Redis Streams start-ID for a brand-new
// group: "$" for Latest, "0" for Earliest, or a specific ID. Has no
// effect when the group already exists (BUSYGROUP error caught above).
func resolveStartID(scfg stream.SubscribeConfig) string {
	if scfg.StartFromID != "" {
		return scfg.StartFromID
	}
	if scfg.StartFrom == stream.StartEarliest {
		return "0"
	}
	return "$"
}

// pendingMsg is one Redis Streams XMessage adapted for our worker
// pipeline.
type pendingMsg struct {
	id       string
	payload  []byte
	headers  map[string]string
	attempts int
}

func (c *consumer) worker(consumerName string) {
	defer c.wg.Done()

	for {
		if c.ctx.Err() != nil {
			return
		}

		// 1. Reclaim stuck-pending first. minIdle = VisibilityTimeout
		// matches the inmem semantics: anything claimed longer ago than
		// VT becomes available to other workers.
		if reclaimed, err := c.reclaim(consumerName); err == nil {
			for _, p := range reclaimed {
				c.runOne(p)
			}
			if len(reclaimed) > 0 {
				continue
			}
		}

		// 2. XREADGROUP for new entries. BLOCK provides low-latency
		// wake-up — no separate notify channel needed.
		batch, err := c.readNew(consumerName)
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			// Transient backoff.
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		for _, p := range batch {
			c.runOne(p)
		}
	}
}

// readNew calls XREADGROUP with BLOCK = pollInterval to fetch up to
// batchSize new entries.
func (c *consumer) readNew(consumerName string) ([]pendingMsg, error) {
	const batchSize = 16
	res, err := c.svc.client.XReadGroup(c.ctx, &rds.XReadGroupArgs{
		Group:    c.group,
		Consumer: consumerName,
		Streams:  []string{streamKey(c.stream), ">"},
		Count:    batchSize,
		Block:    c.cfg.PollInterval,
	}).Result()
	if err != nil {
		if errors.Is(err, rds.Nil) {
			// BLOCK timeout with no new messages.
			return nil, nil
		}
		return nil, fmt.Errorf("xreadgroup: %w", err)
	}
	out := make([]pendingMsg, 0, batchSize)
	for _, st := range res {
		for _, m := range st.Messages {
			out = append(out, fromXMessage(m, 1))
		}
	}
	return out, nil
}

// reclaim runs XAUTOCLAIM with min-idle = VisibilityTimeout. Up to
// batchSize entries are reclaimed per call. The attempts count comes
// from XPENDING (one extra round-trip per reclaimed entry).
func (c *consumer) reclaim(consumerName string) ([]pendingMsg, error) {
	const batchSize = 16
	msgs, _, err := c.svc.client.XAutoClaim(c.ctx, &rds.XAutoClaimArgs{
		Stream:   streamKey(c.stream),
		Group:    c.group,
		Consumer: consumerName,
		MinIdle:  c.cfg.VisibilityTimeout,
		Start:    "0",
		Count:    batchSize,
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]pendingMsg, 0, len(msgs))
	for _, m := range msgs {
		attempts := 1
		pendingArgs := &rds.XPendingExtArgs{
			Stream: streamKey(c.stream),
			Group:  c.group,
			Start:  m.ID,
			End:    m.ID,
			Count:  1,
		}
		if pres, perr := c.svc.client.XPendingExt(c.ctx, pendingArgs).Result(); perr == nil && len(pres) == 1 {
			attempts = int(pres[0].RetryCount)
		}
		out = append(out, fromXMessage(m, attempts))
	}
	return out, nil
}

// fromXMessage converts a Redis XMessage to our internal pendingMsg.
// Reconstructs payload from the "payload" field and headers from any
// "h:*" prefixed fields.
func fromXMessage(m rds.XMessage, attempts int) pendingMsg {
	p := pendingMsg{
		id:       m.ID,
		attempts: attempts,
	}
	headers := make(map[string]string)
	for k, v := range m.Values {
		s, _ := v.(string)
		switch {
		case k == "payload":
			p.payload = []byte(s)
		case strings.HasPrefix(k, "h:"):
			headers[k[2:]] = s
		}
	}
	if len(headers) > 0 {
		p.headers = headers
	}
	return p
}

// runOne builds Message, runs handler, then XACKs (success or DLQ-route)
// or leaves it for XAUTOCLAIM to reclaim later.
func (c *consumer) runOne(p pendingMsg) {
	createdAt := parseStreamIDTime(p.id)
	msg := &stream.Message{
		ID:        p.id,
		Stream:    c.stream,
		Payload:   p.payload,
		Headers:   p.headers,
		CreatedAt: createdAt,
		Attempt:   p.attempts,
	}

	err := safeRun(c.ctx, c.handler, msg)

	if err == nil {
		_ = c.svc.client.XAck(c.ctx, streamKey(c.stream), c.group, p.id).Err()
		return
	}

	skipRetry := errors.Is(err, stream.ErrSkipRetry)
	overBudget := c.cfg.MaxRetries >= 0 && p.attempts > c.cfg.MaxRetries

	if skipRetry || overBudget {
		c.routeDLQ(p)
		return
	}
	// Otherwise leave for XAUTOCLAIM — visibility timeout will expire
	// and the next reclaim cycle will pick it up.
}

// parseStreamIDTime parses a Redis stream ID "ms-seq" and returns the
// timestamp portion. On error returns time.Now() as a safe fallback.
func parseStreamIDTime(id string) time.Time {
	dash := strings.IndexByte(id, '-')
	if dash <= 0 {
		return time.Now()
	}
	ms, err := strconv.ParseInt(id[:dash], 10, 64)
	if err != nil {
		return time.Now()
	}
	return time.UnixMilli(ms)
}

func safeRun(ctx context.Context, h stream.Handler, msg *stream.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("stream/redis: handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, msg)
}

// routeDLQ XADDs the message to the DLQ stream and XACKs the original.
// The two ops aren't atomic, but at-least-once is the contract: a
// crash between them just leaves the message pending in the source
// group — XAUTOCLAIM picks it up on the next cycle.
func (c *consumer) routeDLQ(p pendingMsg) {
	values := make([]any, 0, 2+2*len(p.headers))
	values = append(values, "payload", p.payload)
	for k, v := range p.headers {
		values = append(values, "h:"+k, v)
	}
	if _, err := c.svc.client.XAdd(c.ctx, &rds.XAddArgs{
		Stream: streamKey(c.cfg.DeadLetterStream),
		ID:     "*",
		Values: values,
	}).Result(); err != nil {
		return
	}
	_ = c.svc.client.XAck(c.ctx, streamKey(c.stream), c.group, p.id).Err()
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
