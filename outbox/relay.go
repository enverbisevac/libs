package outbox

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"
)

// Relay polls the Store for pending messages and forwards them to a Publisher.
type Relay struct {
	store  Store
	pub    Publisher
	config Config

	once   sync.Once
	cancel context.CancelFunc
	done   chan struct{}
}

// NewRelay creates a new Relay.
func NewRelay(store Store, pub Publisher, options ...Option) *Relay {
	config := Config{
		PollInterval: time.Second,
		BatchSize:    100,
	}
	for _, opt := range options {
		opt.Apply(&config)
	}
	return &Relay{
		store:  store,
		pub:    pub,
		config: config,
	}
}

// Start begins polling for pending messages. Safe to call multiple times.
func (r *Relay) Start(ctx context.Context) {
	r.once.Do(func() {
		ctx, r.cancel = context.WithCancel(ctx)
		r.done = make(chan struct{})
		go r.run(ctx)
	})
}

// Stop cancels the relay and waits for it to finish.
func (r *Relay) Stop() {
	if r.cancel != nil {
		r.cancel()
		<-r.done
	}
}

func (r *Relay) run(ctx context.Context) {
	defer close(r.done)

	log := logr.FromContextOrDiscard(ctx)
	ticker := time.NewTicker(r.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.poll(ctx, log)
		}
	}
}

func (r *Relay) poll(ctx context.Context, log logr.Logger) {
	msgs, err := r.store.FetchPending(ctx, r.config.BatchSize)
	if err != nil {
		log.Error(err, "outbox: fetch pending")
		return
	}

	if len(msgs) == 0 {
		return
	}

	var processed []string
	for _, msg := range msgs {
		if err := r.pub.Publish(ctx, msg); err != nil {
			log.Error(err, "outbox: publish failed", "id", msg.ID, "topic", msg.Topic)
			if markErr := r.store.MarkFailed(ctx, msg.ID, err); markErr != nil {
				log.Error(markErr, "outbox: mark failed", "id", msg.ID)
			}
			continue
		}
		processed = append(processed, msg.ID)
	}

	if len(processed) > 0 {
		if err := r.store.MarkProcessed(ctx, processed...); err != nil {
			log.Error(err, "outbox: mark processed")
		}
	}
}
