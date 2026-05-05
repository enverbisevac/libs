// Package streambridge adapts an outbox.Publisher to a libs stream.Service,
// so the libs Relay can drain the outbox into a stream broker without any
// project-specific glue.
//
// Convention: Message.Topic is treated as the stream name. Message.Headers
// are forwarded to subscribers via stream.WithHeaders. The well-known key
// "namespace" is also routed through stream.WithPublishNamespace so the
// formatted stream name reflects the originating tenant/account.
package streambridge

import (
	"context"
	"fmt"

	"github.com/enverbisevac/libs/outbox"
	"github.com/enverbisevac/libs/stream"
)

// HeaderNamespace is the well-known Message.Headers key whose value is
// applied as stream.WithPublishNamespace. Any other header keys ride
// along via stream.WithHeaders unchanged.
const HeaderNamespace = "namespace"

var _ outbox.Publisher = (*Publisher)(nil)

// Publisher implements outbox.Publisher on top of a stream.Producer.
type Publisher struct {
	svc stream.Producer
}

// New returns a Publisher backed by svc. Returns nil if svc is nil so
// callers (e.g. wire providers) can pass through a missing dependency
// without an extra branch.
func New(svc stream.Service) outbox.Publisher {
	if svc == nil {
		return nil
	}
	return &Publisher{svc: svc}
}

// Publish forwards each outbox message to the underlying stream.Producer.
//
// In normal operation libs Relay calls Publish with exactly one message,
// so the loop is effectively single-iteration. If a caller passes a batch
// and a publish fails partway, messages [0..i-1] are already on the
// stream; the caller is responsible for reconciliation.
func (p *Publisher) Publish(ctx context.Context, msgs ...outbox.Message) error {
	for _, m := range msgs {
		opts := []stream.PublishOption{}
		if ns := m.Headers[HeaderNamespace]; ns != "" {
			opts = append(opts, stream.WithPublishNamespace(ns))
		}
		if len(m.Headers) > 0 {
			opts = append(opts, stream.WithHeaders(m.Headers))
		}
		if err := p.svc.Publish(ctx, m.Topic, m.Payload, opts...); err != nil {
			return fmt.Errorf("streambridge: publish %q: %w", m.Topic, err)
		}
	}
	return nil
}