package streambridge

import (
	"context"

	"github.com/go-logr/logr"

	"github.com/enverbisevac/libs/outbox"
	"github.com/enverbisevac/libs/stream"
)

// WrapProcessed composes h with per-(group, msg.ID) dedup for non-
// transactional side effects (printing, external API calls). The inner
// handler runs first; on success it is recorded in a follow-up exec. On
// redelivery the dedup check short-circuits before the handler runs again.
//
// Failed inner calls are NOT recorded — retries / redelivery can re-attempt.
// A failure of the recording step itself is logged (not returned) so a rare
// "succeeded but mark failed" path doesn't cause a duplicate side effect on
// redelivery; worst case is one extra exec on the same path next time.
//
// Compose OUTSIDE retry wrappers so dedup short-circuits before retries
// fire on already-processed messages. For transactional side effects, call
// store.MarkProcessed inside your domain transaction directly instead.
//
// db is the driver handle the chosen ProcessedStore implementation
// understands (e.g. *sql.DB for the database/sql backends).
func WrapProcessed(
	store outbox.ProcessedStore,
	db any,
	group string,
	h stream.Handler,
) stream.Handler {
	return func(ctx context.Context, msg *stream.Message) error {
		log := logr.FromContextOrDiscard(ctx)

		processed, err := store.IsProcessed(ctx, db, group, msg.ID)
		if err != nil {
			return err
		}
		if processed {
			log.V(1).Info("stream message already processed, skipping",
				"group", group, "msg_id", msg.ID)
			return nil
		}
		if err := h(ctx, msg); err != nil {
			return err
		}
		if _, err := store.MarkProcessed(ctx, db, group, msg.ID, nil); err != nil {
			log.Error(err, "failed to record processed message; duplicate possible on redelivery",
				"group", group, "msg_id", msg.ID)
		}
		return nil
	}
}