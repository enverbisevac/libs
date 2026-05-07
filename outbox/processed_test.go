package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enverbisevac/libs/outbox"
)

type fakeProcessed struct {
	purges   atomic.Int64
	purgeErr error
	cutoffCh chan time.Time
}

func (f *fakeProcessed) IsProcessed(context.Context, any, string, string) (bool, error) {
	return false, nil
}

func (f *fakeProcessed) MarkProcessed(context.Context, any, string, string, json.RawMessage) (bool, error) {
	return true, nil
}

func (f *fakeProcessed) PurgeProcessedBefore(_ context.Context, _ any, t time.Time) (int64, error) {
	f.purges.Add(1)
	if f.cutoffCh != nil {
		select {
		case f.cutoffCh <- t:
		default:
		}
	}
	if f.purgeErr != nil {
		return 0, f.purgeErr
	}
	return 0, nil
}

func TestRunProcessedCleanup_DisabledOnZeroRetention(t *testing.T) {
	store := &fakeProcessed{}

	done := make(chan struct{})
	go func() {
		outbox.RunProcessedCleanup(context.Background(), store, nil, 0, time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunProcessedCleanup with zero retention should return immediately")
	}
	if got := store.purges.Load(); got != 0 {
		t.Errorf("purges = %d, want 0 (loop should never have started)", got)
	}
}

func TestRunProcessedCleanup_DisabledOnZeroInterval(t *testing.T) {
	store := &fakeProcessed{}

	done := make(chan struct{})
	go func() {
		outbox.RunProcessedCleanup(context.Background(), store, nil, time.Hour, 0)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunProcessedCleanup with zero interval should return immediately")
	}
	if got := store.purges.Load(); got != 0 {
		t.Errorf("purges = %d, want 0", got)
	}
}

func TestRunProcessedCleanup_StopsOnContextCancel(t *testing.T) {
	store := &fakeProcessed{cutoffCh: make(chan time.Time, 1)}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		outbox.RunProcessedCleanup(ctx, store, nil, time.Hour, 5*time.Millisecond)
		close(done)
	}()

	// Wait for at least one purge so we know the loop is alive.
	select {
	case <-store.cutoffCh:
	case <-time.After(time.Second):
		t.Fatal("did not observe a purge tick")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunProcessedCleanup did not return after context cancel")
	}
}

func TestRunProcessedCleanup_ContinuesOnPurgeError(t *testing.T) {
	store := &fakeProcessed{
		purgeErr: errors.New("boom"),
		cutoffCh: make(chan time.Time, 4),
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go outbox.RunProcessedCleanup(ctx, store, nil, time.Hour, 5*time.Millisecond)

	// Two ticks observed → loop survived the error from the first tick.
	for i := range 2 {
		select {
		case <-store.cutoffCh:
		case <-time.After(time.Second):
			t.Fatalf("only observed %d purge ticks before timeout", i)
		}
	}
}

func TestRunProcessedCleanup_CutoffIsRetentionAgo(t *testing.T) {
	store := &fakeProcessed{cutoffCh: make(chan time.Time, 1)}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	retention := 30 * time.Minute
	go outbox.RunProcessedCleanup(ctx, store, nil, retention, 5*time.Millisecond)

	var cutoff time.Time
	select {
	case cutoff = <-store.cutoffCh:
	case <-time.After(time.Second):
		t.Fatal("did not observe a purge tick")
	}

	// The cutoff is time.Now()-retention measured at the tick. Allow a
	// generous delta so a slow CI machine doesn't flake.
	delta := time.Since(cutoff) - retention
	if delta < -time.Second || delta > time.Second {
		t.Errorf("cutoff = %v, expected ~retention(%v) ago, delta = %v", cutoff, retention, delta)
	}
}
