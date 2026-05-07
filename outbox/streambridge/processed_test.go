package streambridge_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/enverbisevac/libs/outbox"
	"github.com/enverbisevac/libs/outbox/sql/sqlite"
	"github.com/enverbisevac/libs/outbox/streambridge"
	"github.com/enverbisevac/libs/stream"
)

func openProcessedDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "wrap.db") +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := sqlite.MigrateProcessed(context.Background(), db, ""); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func TestWrapProcessed_FirstDeliveryRunsAndMarks(t *testing.T) {
	db := openProcessedDB(t)
	store := sqlite.NewProcessed("")

	var calls atomic.Int32
	wrapped := streambridge.WrapProcessed(store, db, "g1",
		func(_ context.Context, _ *stream.Message) error {
			calls.Add(1)
			return nil
		},
	)

	if err := wrapped(context.Background(), &stream.Message{ID: "m1"}); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("handler calls = %d, want 1", got)
	}

	got, err := store.IsProcessed(context.Background(), db, "g1", "m1")
	if err != nil {
		t.Fatalf("IsProcessed: %v", err)
	}
	if !got {
		t.Error("expected message to be marked processed after first call")
	}
}

func TestWrapProcessed_SecondDeliveryShortCircuits(t *testing.T) {
	db := openProcessedDB(t)
	store := sqlite.NewProcessed("")

	var calls atomic.Int32
	wrapped := streambridge.WrapProcessed(store, db, "g1",
		func(_ context.Context, _ *stream.Message) error {
			calls.Add(1)
			return nil
		},
	)

	for range 3 {
		if err := wrapped(context.Background(), &stream.Message{ID: "m1"}); err != nil {
			t.Fatalf("call: %v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("handler calls = %d, want 1 (subsequent deliveries should short-circuit)", got)
	}
}

func TestWrapProcessed_HandlerErrorIsNotMarked(t *testing.T) {
	db := openProcessedDB(t)
	store := sqlite.NewProcessed("")

	wantErr := errors.New("boom")
	wrapped := streambridge.WrapProcessed(store, db, "g1",
		func(_ context.Context, _ *stream.Message) error { return wantErr },
	)

	err := wrapped(context.Background(), &stream.Message{ID: "m1"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}

	got, err := store.IsProcessed(context.Background(), db, "g1", "m1")
	if err != nil {
		t.Fatalf("IsProcessed: %v", err)
	}
	if got {
		t.Error("handler returned an error but message was marked processed; redelivery would skip it")
	}
}

func TestWrapProcessed_PerGroupIndependent(t *testing.T) {
	db := openProcessedDB(t)
	store := sqlite.NewProcessed("")

	var g1Calls, g2Calls atomic.Int32
	mkHandler := func(counter *atomic.Int32) stream.Handler {
		return func(_ context.Context, _ *stream.Message) error {
			counter.Add(1)
			return nil
		}
	}
	w1 := streambridge.WrapProcessed(store, db, "g1", mkHandler(&g1Calls))
	w2 := streambridge.WrapProcessed(store, db, "g2", mkHandler(&g2Calls))

	if err := w1(context.Background(), &stream.Message{ID: "m1"}); err != nil {
		t.Fatalf("g1: %v", err)
	}
	if err := w2(context.Background(), &stream.Message{ID: "m1"}); err != nil {
		t.Fatalf("g2: %v", err)
	}
	if g1Calls.Load() != 1 || g2Calls.Load() != 1 {
		t.Errorf("calls = (g1=%d, g2=%d), want (1, 1)", g1Calls.Load(), g2Calls.Load())
	}
}

// markFailStore wraps the real sqlite store to force MarkProcessed to fail
// after the inner handler succeeds — the documented "succeeded but mark
// failed" path. WrapProcessed must log and return nil so an at-least-once
// redelivery path can re-run the handler at most once more.
type markFailStore struct {
	inner *sqlite.ProcessedStore
}

func (m *markFailStore) IsProcessed(ctx context.Context, db any, group, id string) (bool, error) {
	return m.inner.IsProcessed(ctx, db, group, id)
}

func (m *markFailStore) MarkProcessed(_ context.Context, _ any, _, _ string, _ json.RawMessage) (bool, error) {
	return false, errors.New("mark failed")
}

func (m *markFailStore) PurgeProcessedBefore(ctx context.Context, db any, t time.Time) (int64, error) {
	return m.inner.PurgeProcessedBefore(ctx, db, t)
}

func TestWrapProcessed_MarkFailureLoggedNotReturned(t *testing.T) {
	db := openProcessedDB(t)
	store := &markFailStore{inner: sqlite.NewProcessed("")}

	var calls atomic.Int32
	wrapped := streambridge.WrapProcessed(store, db, "g1",
		func(_ context.Context, _ *stream.Message) error {
			calls.Add(1)
			return nil
		},
	)

	err := wrapped(context.Background(), &stream.Message{ID: "m1"})
	if err != nil {
		t.Errorf("err = %v, want nil (mark failure must be swallowed)", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("handler calls = %d, want 1", got)
	}
}

// Compile-time check: the test fakes still implement the interface.
var _ outbox.ProcessedStore = (*markFailStore)(nil)
