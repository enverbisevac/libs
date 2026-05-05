package streambridge

import (
	"context"
	"errors"
	"testing"

	"github.com/enverbisevac/libs/outbox"
	"github.com/enverbisevac/libs/stream"
)

type fakeCall struct {
	stream  string
	payload []byte
	cfg     stream.PublishConfig
}

type fakeService struct {
	calls []fakeCall
	err   error
}

func (f *fakeService) Publish(_ context.Context, name string, payload []byte, opts ...stream.PublishOption) error {
	cfg := stream.PublishConfig{}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	f.calls = append(f.calls, fakeCall{
		stream:  name,
		payload: append([]byte(nil), payload...),
		cfg:     cfg,
	})
	return f.err
}

func (f *fakeService) Subscribe(context.Context, string, string, stream.Handler, ...stream.SubscribeOption) (stream.Consumer, error) {
	panic("not used")
}

func (f *fakeService) Close(context.Context) error { return nil }

func TestNew_NilServiceReturnsNil(t *testing.T) {
	if got := New(nil); got != nil {
		t.Fatalf("New(nil) = %v, want nil", got)
	}
}

func TestPublish_RestoresNamespaceAndForwardsHeaders(t *testing.T) {
	svc := &fakeService{}
	pub := New(svc)
	if pub == nil {
		t.Fatal("expected non-nil publisher")
	}

	err := pub.Publish(context.Background(), outbox.Message{
		ID:      "row-1",
		Topic:   "stream",
		Payload: []byte(`{"event":"x"}`),
		Headers: map[string]string{HeaderNamespace: "acct-1", "trace": "abc"},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := len(svc.calls); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
	c := svc.calls[0]
	if c.stream != "stream" {
		t.Errorf("stream = %q, want %q", c.stream, "stream")
	}
	if string(c.payload) != `{"event":"x"}` {
		t.Errorf("payload = %q, want %q", string(c.payload), `{"event":"x"}`)
	}
	if c.cfg.Namespace != "acct-1" {
		t.Errorf("Namespace = %q, want %q", c.cfg.Namespace, "acct-1")
	}
	if got, want := c.cfg.Headers["trace"], "abc"; got != want {
		t.Errorf("Headers[trace] = %q, want %q", got, want)
	}
}

func TestPublish_MissingNamespaceHeader_StillPublishes(t *testing.T) {
	svc := &fakeService{}
	pub := New(svc)

	err := pub.Publish(context.Background(), outbox.Message{
		Topic:   "stream",
		Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := len(svc.calls); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
	if c := svc.calls[0]; c.cfg.Namespace != "" {
		t.Errorf("Namespace = %q, want empty", c.cfg.Namespace)
	}
}

func TestPublish_PublishErrorIsWrapped(t *testing.T) {
	svc := &fakeService{err: errors.New("broker down")}
	pub := New(svc)

	err := pub.Publish(context.Background(), outbox.Message{Topic: "topic-x", Payload: []byte("x")})
	if err == nil {
		t.Fatal("Publish: expected error, got nil")
	}
	if got := err.Error(); got == "" {
		t.Errorf("error message empty")
	}
}