package pgx

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/enverbisevac/libs/pubsub"
	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

type command struct {
	sql      string
	resultCh chan error
}

type PubSub struct {
	config Config
	pool   *pgxpool.Pool
	db     *sql.DB
	conn   *pgx.Conn

	mutex       sync.RWMutex
	subscribers []*pgxSubscriber

	// listener management
	startOnce   sync.Once
	cmdChan     chan command
	cancelWait  context.CancelFunc
	listenerCtx context.Context
	cancel      context.CancelFunc
	done        chan struct{}
}

// New create an instance of memory pubsub implementation.
func New(
	ctx context.Context,
	pool *pgxpool.Pool,
	options ...Option,
) (*PubSub, error) {
	config := Config{
		App:         pubsub.DefaultAppName,
		Namespace:   pubsub.DefaultNamespace,
		SendTimeout: 10 * time.Second,
		ChannelSize: 100,
	}

	for _, f := range options {
		f.Apply(&config)
	}

	poolConn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return &PubSub{
		config:  config,
		pool:    pool,
		conn:    poolConn.Conn(),
		cmdChan: make(chan command, 16),
	}, nil
}

// NewStdLib create an instance of memory pubsub implementation.
func NewStdLib(
	ctx context.Context,
	db *sql.DB,
	options ...Option,
) (*PubSub, error) {
	config := Config{
		App:         pubsub.DefaultAppName,
		Namespace:   pubsub.DefaultNamespace,
		SendTimeout: 10 * time.Second,
		ChannelSize: 100,
	}

	for _, f := range options {
		f.Apply(&config)
	}

	var conn *pgx.Conn
	var err error

	dbConn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	err = dbConn.Raw(func(driverConn any) error {
		conn = driverConn.(*stdlib.Conn).Conn()
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &PubSub{
		config:  config,
		conn:    conn,
		db:      db,
		cmdChan: make(chan command, 16),
	}, nil
}

func (ps *PubSub) Close(ctx context.Context) error {
	if ps.cancel != nil {
		ps.cancel()
		<-ps.done
	}

	ps.mutex.Lock()
	for _, sub := range ps.subscribers {
		_ = sub.Close()
	}
	ps.mutex.Unlock()

	return ps.conn.Close(ctx)
}

func (ps *PubSub) ensureListenerStarted(ctx context.Context) {
	ps.startOnce.Do(func() {
		ps.listenerCtx, ps.cancel = context.WithCancel(ctx)
		ps.done = make(chan struct{})
		go ps.listen()
	})
}

func (ps *PubSub) listen() {
	log := logr.FromContextOrDiscard(ps.listenerCtx)
	defer close(ps.done)

	for {
		// process any pending commands first
		ps.processPendingCommands(log)

		// check if we should stop
		if ps.listenerCtx.Err() != nil {
			return
		}

		// create a cancellable context for the wait
		waitCtx, cancelWait := context.WithCancel(ps.listenerCtx)
		ps.mutex.Lock()
		ps.cancelWait = cancelWait
		ps.mutex.Unlock()

		notification, err := ps.conn.WaitForNotification(waitCtx)
		cancelWait()

		if err != nil {
			if ps.listenerCtx.Err() != nil {
				return
			}
			// context was cancelled to process new commands, continue loop
			if waitCtx.Err() != nil {
				continue
			}
			log.Error(err, "failed to wait for notification")
			time.Sleep(20 * time.Millisecond)
			continue
		}

		msg := &pubsub.Msg{
			Topic:   notification.Channel,
			Payload: []byte(notification.Payload),
		}

		ps.broadcast(msg, log)
	}
}

func (ps *PubSub) processPendingCommands(log logr.Logger) {
	for {
		select {
		case cmd := <-ps.cmdChan:
			_, err := ps.conn.Exec(ps.listenerCtx, cmd.sql)
			if err != nil {
				log.Error(err, "failed to execute command", "sql", cmd.sql)
			}
			cmd.resultCh <- err
		default:
			return
		}
	}
}

func (ps *PubSub) execCommand(ctx context.Context, sql string) error {
	resultCh := make(chan error, 1)
	cmd := command{sql: sql, resultCh: resultCh}

	select {
	case ps.cmdChan <- cmd:
	case <-ctx.Done():
		return ctx.Err()
	}

	// interrupt the wait so command gets processed
	ps.mutex.RLock()
	if ps.cancelWait != nil {
		ps.cancelWait()
	}
	ps.mutex.RUnlock()

	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ps *PubSub) broadcast(msg *pubsub.Msg, log logr.Logger) {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	for _, sub := range ps.subscribers {
		if !sub.matches(msg.Topic) {
			continue
		}

		if sub.handler != nil {
			if err := sub.handler(msg); err != nil {
				log.Error(err, "received an error from handler function")
			}
		} else if sub.channel != nil {
			select {
			case sub.channel <- msg:
			case <-time.After(sub.config.SendTimeout):
				log.V(1).Info("timeout sending to subscriber channel", "topic", msg.Topic)
			}
		}
	}
}

func (ps *PubSub) Publish(
	ctx context.Context,
	topic string,
	payload []byte,
	opts ...pubsub.PublishOption,
) error {
	log := logr.FromContextOrDiscard(ctx)

	ctx = context.WithoutCancel(ctx)

	log.V(1).Info("publishing event", "topic", topic)

	pubConfig := pubsub.PublishConfig{
		App:       ps.config.App,
		Namespace: ps.config.Namespace,
	}
	for _, f := range opts {
		f.Apply(&pubConfig)
	}

	topic = pubsub.FormatTopic(pubConfig.App, pubConfig.Namespace, topic)

	sqlQuery := fmt.Sprintf("NOTIFY %q, '%s'", topic, payload)

	if ps.pool != nil {
		conn, err := ps.pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("failed to acquire connection: %w", err)
		}
		defer conn.Release()

		_, err = conn.Exec(ctx, sqlQuery)
		if err != nil {
			return fmt.Errorf("failed to publish event: %w", err)
		}
	} else if ps.db != nil {
		_, err := ps.db.ExecContext(ctx, sqlQuery)
		if err != nil {
			return fmt.Errorf("failed to publish event: %w", err)
		}
	}

	return nil
}

// subscribe consumer to process the event with payload.
func (ps *PubSub) subscribe(
	ctx context.Context,
	topic string,
	options ...pubsub.SubscribeOption,
) (*pgxSubscriber, error) {
	config := pubsub.SubscribeConfig{
		Topics:      make([]string, 0, 8),
		App:         ps.config.App,
		Namespace:   ps.config.Namespace,
		SendTimeout: ps.config.SendTimeout,
		ChannelSize: ps.config.ChannelSize,
	}

	for _, f := range options {
		f.Apply(&config)
	}

	// start listener if not already running
	ps.ensureListenerStarted(ctx)

	// format topic with app/namespace prefix
	formattedTopic := pubsub.FormatTopic(config.App, config.Namespace, topic)
	config.Topics = append(config.Topics, formattedTopic)

	// execute LISTEN command via the listener goroutine
	sqlQuery := fmt.Sprintf("LISTEN %q", formattedTopic)
	if err := ps.execCommand(ctx, sqlQuery); err != nil {
		return nil, fmt.Errorf("failed to listen to topic: %w", err)
	}

	subscriber := &pgxSubscriber{
		ps:     ps,
		config: config,
	}

	ps.mutex.Lock()
	ps.subscribers = append(ps.subscribers, subscriber)
	ps.mutex.Unlock()

	return subscriber, nil
}

// Subscribe consumer to process the event with payload.
func (ps *PubSub) Subscribe(
	ctx context.Context,
	topic string,
	handler func(msg *pubsub.Msg) error,
	options ...pubsub.SubscribeOption,
) pubsub.Consumer {
	subscriber, err := ps.subscribe(ctx, topic, options...)
	if err != nil {
		return nil
	}
	subscriber.handler = handler
	return subscriber
}

func (ps *PubSub) SubscribeChan(
	ctx context.Context,
	topic string,
	options ...pubsub.SubscribeOption,
) (pubsub.Consumer, <-chan *pubsub.Msg) {
	subscriber, err := ps.subscribe(ctx, topic, options...)
	if err != nil {
		return nil, nil
	}
	subscriber.channel = make(chan *pubsub.Msg, subscriber.config.ChannelSize)
	return subscriber, subscriber.channel
}

type pgxSubscriber struct {
	ps      *PubSub
	handler func(msg *pubsub.Msg) error

	mutex   sync.RWMutex
	channel chan *pubsub.Msg

	config pubsub.SubscribeConfig

	once   sync.Once
	closed bool
}

func (s *pgxSubscriber) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if s.channel != nil {
		s.once.Do(func() {
			close(s.channel)
		})
	}

	return nil
}

func (s *pgxSubscriber) matches(topic string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.closed {
		return false
	}

	return slices.Contains(s.config.Topics, topic)
}

func (s *pgxSubscriber) Subscribe(
	ctx context.Context,
	topics ...string,
) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, topic := range topics {
		formattedTopic := pubsub.FormatTopic(s.config.App, s.config.Namespace, topic)
		if slices.Contains(s.config.Topics, formattedTopic) {
			continue
		}

		// execute LISTEN command via the listener goroutine
		sqlQuery := fmt.Sprintf("LISTEN %q", formattedTopic)
		if err := s.ps.execCommand(ctx, sqlQuery); err != nil {
			return fmt.Errorf("failed to listen to topic: %w", err)
		}

		s.config.Topics = append(s.config.Topics, formattedTopic)
	}
	return nil
}

func (s *pgxSubscriber) Unsubscribe(
	ctx context.Context,
	topics ...string,
) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, topic := range topics {
		formattedTopic := pubsub.FormatTopic(s.config.App, s.config.Namespace, topic)
		idx := slices.Index(s.config.Topics, formattedTopic)
		if idx == -1 {
			continue
		}

		// execute UNLISTEN command via the listener goroutine
		sqlQuery := fmt.Sprintf("UNLISTEN %q", formattedTopic)
		if err := s.ps.execCommand(ctx, sqlQuery); err != nil {
			return fmt.Errorf("failed to unlisten from topic: %w", err)
		}

		s.config.Topics = slices.Delete(s.config.Topics, idx, idx+1)
	}
	return nil
}