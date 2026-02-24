package inmem

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enverbisevac/libs/pubsub"
)

func TestNew(t *testing.T) {
	ps := New()
	if ps.config.App != pubsub.DefaultAppName {
		t.Errorf("expected app %q, got %q", pubsub.DefaultAppName, ps.config.App)
	}
	if ps.config.Namespace != pubsub.DefaultNamespace {
		t.Errorf("expected namespace %q, got %q", pubsub.DefaultNamespace, ps.config.Namespace)
	}
	if ps.config.SendTimeout != 10*time.Second {
		t.Errorf("expected send timeout %v, got %v", 10*time.Second, ps.config.SendTimeout)
	}
	if ps.config.ChannelSize != 100 {
		t.Errorf("expected channel size 100, got %d", ps.config.ChannelSize)
	}
}

func TestNewWithOptions(t *testing.T) {
	ps := New(
		WithApp("myapp"),
		WithNamespace("prod"),
		WithSendTimeout(5*time.Second),
		WithSize(50),
	)
	if ps.config.App != "myapp" {
		t.Errorf("expected app %q, got %q", "myapp", ps.config.App)
	}
	if ps.config.Namespace != "prod" {
		t.Errorf("expected namespace %q, got %q", "prod", ps.config.Namespace)
	}
	if ps.config.SendTimeout != 5*time.Second {
		t.Errorf("expected send timeout %v, got %v", 5*time.Second, ps.config.SendTimeout)
	}
	if ps.config.ChannelSize != 50 {
		t.Errorf("expected channel size 50, got %d", ps.config.ChannelSize)
	}
}

func TestSubscribeAndPublish(t *testing.T) {
	ps := New()
	ctx := context.Background()

	var received atomic.Value
	done := make(chan struct{})

	ps.Subscribe(ctx, "test-topic", func(msg *pubsub.Msg) error {
		received.Store(string(msg.Payload))
		close(done)
		return nil
	})

	err := ps.Publish(ctx, "test-topic", []byte("hello"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	if got := received.Load().(string); got != "hello" {
		t.Errorf("expected payload %q, got %q", "hello", got)
	}
}

func TestSubscribeChan(t *testing.T) {
	ps := New()
	ctx := context.Background()

	consumer, ch := ps.SubscribeChan(ctx, "events")

	err := ps.Publish(ctx, "events", []byte("data"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case msg := <-ch:
		if string(msg.Payload) != "data" {
			t.Errorf("expected payload %q, got %q", "data", string(msg.Payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message on channel")
	}

	if err := consumer.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestPublishNoSubscribers(t *testing.T) {
	ps := New()
	ctx := context.Background()

	err := ps.Publish(ctx, "no-one-listening", []byte("ignored"))
	if err != nil {
		t.Fatalf("expected no error publishing with no subscribers, got: %v", err)
	}
}

func TestPublishWrongTopic(t *testing.T) {
	ps := New()
	ctx := context.Background()

	var called atomic.Bool
	ps.Subscribe(ctx, "topic-a", func(msg *pubsub.Msg) error {
		called.Store(true)
		return nil
	})

	err := ps.Publish(ctx, "topic-b", []byte("data"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// give some time for any accidental delivery
	time.Sleep(100 * time.Millisecond)

	if called.Load() {
		t.Error("handler was called for wrong topic")
	}
}

func TestMultipleSubscribers(t *testing.T) {
	ps := New()
	ctx := context.Background()

	var count atomic.Int32
	done := make(chan struct{})

	for range 3 {
		ps.Subscribe(ctx, "shared", func(msg *pubsub.Msg) error {
			if count.Add(1) == 3 {
				close(done)
			}
			return nil
		})
	}

	err := ps.Publish(ctx, "shared", []byte("broadcast"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out: only %d/3 subscribers received", count.Load())
	}
}

func TestConsumerSubscribeAddsTopics(t *testing.T) {
	ps := New()
	ctx := context.Background()

	consumer, ch := ps.SubscribeChan(ctx, "initial")

	err := consumer.Subscribe(ctx, "extra")
	if err != nil {
		t.Fatalf("subscribe to extra topic failed: %v", err)
	}

	err = ps.Publish(ctx, "extra", []byte("extra-msg"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case msg := <-ch:
		if string(msg.Payload) != "extra-msg" {
			t.Errorf("expected %q, got %q", "extra-msg", string(msg.Payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message on extra topic")
	}

	_ = consumer.Close()
}

func TestConsumerUnsubscribeRemovesTopic(t *testing.T) {
	ps := New()
	ctx := context.Background()

	consumer, _ := ps.SubscribeChan(ctx, "removable")

	err := consumer.Unsubscribe(ctx, "removable")
	if err != nil {
		t.Fatalf("unsubscribe failed: %v", err)
	}

	// after unsubscribe, the subscriber should not have the topic
	sub := consumer.(*inMemorySubscriber)
	formattedTopic := pubsub.FormatTopic(pubsub.DefaultAppName, pubsub.DefaultNamespace, "removable")
	if sub.HasTopic(formattedTopic) {
		t.Error("subscriber still has topic after unsubscribe")
	}

	_ = consumer.Close()
}

func TestConsumerClose(t *testing.T) {
	ps := New()
	ctx := context.Background()

	consumer, _ := ps.SubscribeChan(ctx, "closeme")

	ps.mutex.RLock()
	if len(ps.registry) != 1 {
		t.Fatalf("expected 1 subscriber in registry, got %d", len(ps.registry))
	}
	ps.mutex.RUnlock()

	err := consumer.Close()
	if err != nil {
		t.Fatalf("close failed: %v", err)
	}

	ps.mutex.RLock()
	if len(ps.registry) != 0 {
		t.Errorf("expected 0 subscribers after close, got %d", len(ps.registry))
	}
	ps.mutex.RUnlock()
}

func TestConsumerDoubleClose(t *testing.T) {
	ps := New()
	ctx := context.Background()

	consumer, _ := ps.SubscribeChan(ctx, "double")

	if err := consumer.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}
	if err := consumer.Close(); err != nil {
		t.Fatalf("second close should be no-op, got: %v", err)
	}
}

func TestPublishToClosedSubscriber(t *testing.T) {
	ps := New()
	ctx := context.Background()

	consumer, _ := ps.SubscribeChan(ctx, "closed-topic")
	_ = consumer.Close()

	// should not error — closed subscriber is skipped
	err := ps.Publish(ctx, "closed-topic", []byte("ignored"))
	if err != nil {
		t.Fatalf("publish to closed subscriber should not fail, got: %v", err)
	}
}

func TestPubSubClose(t *testing.T) {
	ps := New()
	ctx := context.Background()

	ps.SubscribeChan(ctx, "t1")
	ps.SubscribeChan(ctx, "t2")
	ps.SubscribeChan(ctx, "t3")

	ps.mutex.RLock()
	if len(ps.registry) != 3 {
		t.Fatalf("expected 3 subscribers, got %d", len(ps.registry))
	}
	ps.mutex.RUnlock()

	err := ps.Close(ctx)
	if err != nil {
		t.Fatalf("close failed: %v", err)
	}

	ps.mutex.RLock()
	if len(ps.registry) != 0 {
		t.Errorf("expected 0 subscribers after PubSub.Close, got %d", len(ps.registry))
	}
	ps.mutex.RUnlock()
}

func TestPubSubCloseNoDeadlock(t *testing.T) {
	ps := New()
	ctx := context.Background()

	for range 10 {
		ps.SubscribeChan(ctx, "deadlock-test")
	}

	done := make(chan struct{})
	go func() {
		_ = ps.Close(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("PubSub.Close deadlocked")
	}
}

func TestPublishTimeout(t *testing.T) {
	ps := New(WithSendTimeout(50*time.Millisecond), WithSize(1))
	ctx := context.Background()

	// use channel subscriber so messages are not consumed
	_, _ = ps.SubscribeChan(ctx, "slow")

	// fill the channel buffer
	err := ps.Publish(ctx, "slow", []byte("first"))
	if err != nil {
		t.Fatalf("first publish failed: %v", err)
	}

	// next publish should timeout since buffer is full and no one reads
	err = ps.Publish(ctx, "slow", []byte("second"))
	if err == nil {
		t.Fatal("expected timeout error on full channel, got nil")
	}
}

func TestConcurrentPublish(t *testing.T) {
	ps := New()
	ctx := context.Background()

	var count atomic.Int32
	var wg sync.WaitGroup

	ps.Subscribe(ctx, "concurrent", func(msg *pubsub.Msg) error {
		count.Add(1)
		return nil
	})

	// allow the goroutine to start
	time.Sleep(50 * time.Millisecond)

	n := 50
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_ = ps.Publish(ctx, "concurrent", []byte("msg"))
		}()
	}

	wg.Wait()
	// wait for handler processing
	time.Sleep(500 * time.Millisecond)

	if got := count.Load(); got != int32(n) {
		t.Errorf("expected %d messages, got %d", n, got)
	}
}

func TestSubscribeDuplicateTopic(t *testing.T) {
	ps := New()
	ctx := context.Background()

	consumer, _ := ps.SubscribeChan(ctx, "dup")

	// subscribing to the same topic again should not add a duplicate
	err := consumer.Subscribe(ctx, "dup")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	sub := consumer.(*inMemorySubscriber)
	sub.mutex.RLock()
	topicCount := len(sub.topics)
	sub.mutex.RUnlock()

	if topicCount != 1 {
		t.Errorf("expected 1 topic (no duplicate), got %d", topicCount)
	}

	_ = consumer.Close()
}

func TestPublishWithCancelledContext(t *testing.T) {
	ps := New()
	ctx, cancel := context.WithCancel(context.Background())

	_, _ = ps.SubscribeChan(ctx, "cancel-test")

	cancel()

	// context.WithoutCancel is used inside Publish, so it should still work
	err := ps.Publish(ctx, "cancel-test", []byte("after-cancel"))
	if err != nil {
		t.Fatalf("expected publish to succeed with cancelled parent context, got: %v", err)
	}
}

func TestHandlerError(t *testing.T) {
	ps := New()
	ctx := context.Background()

	done := make(chan struct{})

	ps.Subscribe(ctx, "err-topic", func(msg *pubsub.Msg) error {
		defer close(done)
		return &testError{"handler failed"}
	})

	err := ps.Publish(ctx, "err-topic", []byte("trigger"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case <-done:
		// handler ran and returned error — logged but not propagated
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handler")
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
