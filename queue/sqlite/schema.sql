CREATE TABLE IF NOT EXISTS queue_jobs (
    id           TEXT PRIMARY KEY,
    topic        TEXT NOT NULL,
    payload      BLOB NOT NULL,
    priority     INTEGER NOT NULL DEFAULT 0,
    attempt      INTEGER NOT NULL DEFAULT 0,
    run_at       INTEGER NOT NULL,
    locked_until INTEGER,
    last_error   TEXT,
    enqueued_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS queue_jobs_ready_idx
    ON queue_jobs (topic, priority DESC, run_at ASC, enqueued_at ASC)
    WHERE locked_until IS NULL;

CREATE INDEX IF NOT EXISTS queue_jobs_locked_idx
    ON queue_jobs (locked_until)
    WHERE locked_until IS NOT NULL;