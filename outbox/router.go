package outbox

import (
	"context"
	"fmt"
)

var _ Publisher = (*RouterPublisher)(nil)

// HeaderDestination is the conventional Message.Headers key used by
// WithHeaders/WithDestination to select a destination publisher. Producers
// set this header at write time so the router can dispatch without inspecting
// the topic name.
const HeaderDestination = "destination"

// RouteFunc selects a Publisher for a given outbox Message. Return nil to
// indicate "no match" — the router will try the next configured rule, or
// the fallback if no rule matches.
type RouteFunc func(Message) Publisher

// RouterPublisher dispatches each outbox message to a Publisher chosen by
// caller-supplied rules. Rules are evaluated in registration order; the first
// rule whose RouteFunc returns a non-nil Publisher wins. If no rule matches,
// the fallback is used; if no fallback is configured, Publish returns an
// error so misrouted messages stay in the outbox for investigation rather
// than being silently dropped.
//
// Use NewRouter with functional options to compose:
//
//	router := outbox.NewRouter(
//	    outbox.WithDestination(map[string]outbox.Publisher{
//	        "queue": workQueueAdapter,
//	    }),
//	    outbox.WithFallback(eventsAdapter),
//	)
type RouterPublisher struct {
	rules    []RouteFunc
	fallback Publisher
}

// RouterOption configures a RouterPublisher.
type RouterOption interface {
	Apply(*RouterPublisher)
}

// RouterOptionFunc adapts a function to RouterOption.
type RouterOptionFunc func(*RouterPublisher)

// Apply calls f(r).
func (f RouterOptionFunc) Apply(r *RouterPublisher) { f(r) }

// NewRouter creates a RouterPublisher from the given options. With no
// options the router rejects every message; supply at least one rule
// (WithRoute / WithHeaders / WithDestination) and/or a fallback
// (WithFallback).
func NewRouter(opts ...RouterOption) *RouterPublisher {
	r := &RouterPublisher{}
	for _, o := range opts {
		o.Apply(r)
	}
	return r
}

// WithRoute appends an arbitrary RouteFunc. Use this for custom dispatch
// logic the built-in helpers do not cover.
func WithRoute(f RouteFunc) RouterOption {
	return RouterOptionFunc(func(r *RouterPublisher) {
		if f != nil {
			r.rules = append(r.rules, f)
		}
	})
}

// WithHeaders appends a rule that dispatches based on a Message Header value.
// The router looks up m.Headers[key] in byName and returns the matching
// Publisher; on miss the rule returns nil so the next rule (or fallback) is
// tried.
func WithHeaders(key string, byName map[string]Publisher) RouterOption {
	return RouterOptionFunc(func(r *RouterPublisher) {
		if key == "" || len(byName) == 0 {
			return
		}
		r.rules = append(r.rules, func(m Message) Publisher {
			if m.Headers == nil {
				return nil
			}
			return byName[m.Headers[key]]
		})
	})
}

// WithDestination is a shorthand for WithHeaders(HeaderDestination, byName).
// This is the recommended pattern for multi-destination outboxes: producers
// set Headers["destination"] at write time and the router dispatches by
// looking it up in the supplied map.
func WithDestination(byName map[string]Publisher) RouterOption {
	return WithHeaders(HeaderDestination, byName)
}

// WithFallback sets the Publisher used when no rule matches. Without a
// fallback, unmatched messages cause Publish to return an error and the
// relay will retry them on the next poll.
func WithFallback(p Publisher) RouterOption {
	return RouterOptionFunc(func(r *RouterPublisher) {
		r.fallback = p
	})
}

// Publish dispatches each message individually so a single batch can land in
// multiple backends. The first dispatch error is returned and stops the loop;
// already-published messages are not rolled back. The relay's per-message
// MarkProcessed/MarkFailed bookkeeping (see Relay.poll) handles partial
// success at the message level — failed messages stay in the outbox for
// retry on the next poll.
func (r *RouterPublisher) Publish(ctx context.Context, msgs ...Message) error {
	for _, m := range msgs {
		pub := r.resolve(m)
		if pub == nil {
			return fmt.Errorf("outbox: router: no publisher matched message id=%s topic=%s", m.ID, m.Topic)
		}
		if err := pub.Publish(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

func (r *RouterPublisher) resolve(m Message) Publisher {
	for _, rule := range r.rules {
		if p := rule(m); p != nil {
			return p
		}
	}
	return r.fallback
}
