package outbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-logr/logr"
)

// ProcessedStore is the consumer-side mirror of Store: at-least-once stream
// delivery means a handler can run twice on the same message; ProcessedStore
// records (group, message_id) so handlers short-circuit on redelivery.
//
// db is driver-specific: pass *sql.DB / *sql.Tx for the database/sql
// backends, pgx.Tx / pgxpool.Pool for a pgx backend. Each implementation
// type-asserts the supported handles. Pass a transaction handle to compose
// IsProcessed and MarkProcessed inside a domain transaction (transactional
// exactly-once); pass the pool/DB handle for read-only checks and the
// cleanup loop.
//
// Pattern for transactional exactly-once:
//
//	tx, _ := db.BeginTx(ctx, nil)
//	defer tx.Rollback()
//	isNew, err := store.MarkProcessed(ctx, tx, group, msg.ID, nil)
//	if err != nil { return err }
//	if !isNew { return nil }
//	// ... domain ops in tx ...
//	return tx.Commit()
//
// On rollback the mark is released; on commit it persists with the domain
// writes atomically.
type ProcessedStore interface {
	// IsProcessed reports whether (group, msgID) is already recorded.
	IsProcessed(ctx context.Context, db any, group, msgID string) (bool, error)

	// MarkProcessed records (group, msgID). Returns isNew=true on first
	// insert; false if a row already existed (treat as "skip — already
	// done"). Concurrent inserts are absorbed via ON CONFLICT DO NOTHING.
	//
	// metadata may be nil. When non-nil it must be valid JSON; the
	// implementation validates and returns an error otherwise. Free-form
	// so callers can attach handler-specific context (printed printer
	// names, snapshot of config, audit trail) without further migrations.
	MarkProcessed(ctx context.Context, db any, group, msgID string, metadata json.RawMessage) (isNew bool, err error)

	// PurgeProcessedBefore deletes rows whose processed_at is strictly
	// before t. Match the outbox retention so the dedup window stays at
	// least as wide as the message redelivery window.
	PurgeProcessedBefore(ctx context.Context, db any, t time.Time) (int64, error)
}

// RunProcessedCleanup blocks until ctx is canceled, periodically purging
// rows older than retention. Run as a goroutine.
//
// A non-positive retention or interval disables the loop (returns
// immediately, after logging at V(1) so misconfiguration is observable).
// Errors from PurgeProcessedBefore are logged via logr and do not stop the
// loop.
func RunProcessedCleanup(
	ctx context.Context,
	store ProcessedStore,
	db any,
	retention, interval time.Duration,
) {
	log := logr.FromContextOrDiscard(ctx)
	if retention <= 0 || interval <= 0 {
		log.V(1).Info("outbox: processed cleanup disabled",
			"retention", retention, "interval", interval)
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-retention)
			if _, err := store.PurgeProcessedBefore(ctx, db, cutoff); err != nil {
				log.Error(err, "outbox: processed cleanup")
			}
		}
	}
}
