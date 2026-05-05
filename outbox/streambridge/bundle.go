package streambridge

import (
	"context"
	"errors"

	"github.com/enverbisevac/libs/outbox"
	"github.com/enverbisevac/libs/stream"
)

// Bundle is a fully-wired outbox + stream + relay. It owns the lifecycle
// of the Relay (and, for per-backend constructors like NewSQLite /
// NewPostgres, of the Stream service and any derived resources).
//
// Typical usage:
//
//	b, err := streambridge.NewSQLite(ctx, db, streambridge.SQLiteConfig{})
//	if err != nil { ... }
//	b.Start(ctx)
//	defer b.Stop(ctx)
//
//	// Transactional outbox write:
//	_ = b.Save(ctx, tx, outbox.Message{Topic: "events", Payload: ...})
//
//	// Stream consumer:
//	cons, _ := b.Subscribe(ctx, "events", "g1", handler)
//	defer cons.Close()
//
// Store, Stream, and Relay are exposed for callers that need direct
// access (e.g. specialised options on Subscribe). Do not Close the
// Stream or Stop the Relay yourself — call Bundle.Stop instead.
type Bundle struct {
	Store  outbox.Store
	Stream stream.Service
	Relay  *outbox.Relay

	// closers run in reverse order during Stop. Used to release any
	// resources that the per-backend constructors created on the
	// caller's behalf (e.g. the *sql.DB derived from a pgxpool.Pool).
	closers []func() error
}

// New wires an existing Store + Service via the streambridge Publisher
// and returns a Bundle. The Relay is constructed but NOT started — call
// Start to begin draining.
//
// The caller continues to own store and svc. Bundle.Stop will Close svc
// (since the relay needs the stream to publish) but will not touch any
// resource backing the Store; close those yourself.
func New(store outbox.Store, svc stream.Service, opts ...outbox.Option) *Bundle {
	pub := NewPublisher(svc)
	return &Bundle{
		Store:  store,
		Stream: svc,
		Relay:  outbox.NewRelay(store, pub, opts...),
	}
}

// Start launches the relay. Idempotent.
func (b *Bundle) Start(ctx context.Context) {
	b.Relay.Start(ctx)
}

// Stop stops the relay, closes the stream service, and releases any
// resources owned by per-backend constructors. Safe to call once;
// subsequent calls are no-ops aside from re-running Stream.Close (which
// is itself idempotent on every backend).
func (b *Bundle) Stop(ctx context.Context) error {
	b.Relay.Stop()
	var errs []error
	if err := b.Stream.Close(ctx); err != nil {
		errs = append(errs, err)
	}
	for i := len(b.closers) - 1; i >= 0; i-- {
		if err := b.closers[i](); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Save persists outbox messages within tx. tx may be nil to let the
// underlying Store create its own transaction.
func (b *Bundle) Save(ctx context.Context, tx any, msgs ...outbox.Message) error {
	return b.Store.Save(ctx, tx, msgs...)
}

// Publish sends a message directly via the stream service, BYPASSING the
// outbox. Use Save + Start for transactional guarantees; use Publish
// when you don't need the outbox's at-least-once boundary (e.g. system
// events that are fine to drop on crash).
func (b *Bundle) Publish(ctx context.Context, name string, payload []byte, opts ...stream.PublishOption) error {
	return b.Stream.Publish(ctx, name, payload, opts...)
}

// Subscribe attaches a handler to a stream. Forwarded verbatim to the
// underlying stream.Service; see the stream package for option semantics.
func (b *Bundle) Subscribe(
	ctx context.Context,
	name, group string,
	h stream.Handler,
	opts ...stream.SubscribeOption,
) (stream.Consumer, error) {
	return b.Stream.Subscribe(ctx, name, group, h, opts...)
}
