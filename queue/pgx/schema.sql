CREATE TABLE IF NOT EXISTS queue_jobs (
    id           UUID PRIMARY KEY,
    topic        TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    priority     SMALLINT NOT NULL DEFAULT 0,
    attempt      INT NOT NULL DEFAULT 0,
    run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at    TIMESTAMPTZ,
    locked_until TIMESTAMPTZ,
    last_error   TEXT,
    enqueued_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS queue_jobs_ready_idx
    ON queue_jobs (topic, priority DESC, run_at ASC, enqueued_at ASC)
    WHERE locked_until IS NULL;

CREATE INDEX IF NOT EXISTS queue_jobs_locked_idx
    ON queue_jobs (locked_until)
    WHERE locked_until IS NOT NULL;