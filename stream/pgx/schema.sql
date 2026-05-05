CREATE TABLE IF NOT EXISTS stream_messages (
    id          BIGSERIAL PRIMARY KEY,
    stream      TEXT NOT NULL,
    payload     BYTEA NOT NULL,
    headers     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS stream_messages_stream_id_idx
    ON stream_messages (stream, id);

CREATE INDEX IF NOT EXISTS stream_messages_stream_created_idx
    ON stream_messages (stream, created_at);

CREATE TABLE IF NOT EXISTS stream_groups (
    stream     TEXT NOT NULL,
    group_name TEXT NOT NULL,
    last_id    BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream, group_name)
);

CREATE TABLE IF NOT EXISTS stream_pending (
    stream         TEXT NOT NULL,
    group_name     TEXT NOT NULL,
    msg_id         BIGINT NOT NULL,
    consumer       TEXT NOT NULL,
    claim_deadline TIMESTAMPTZ NOT NULL,
    attempts       INT NOT NULL DEFAULT 1,
    PRIMARY KEY (stream, group_name, msg_id)
);

CREATE INDEX IF NOT EXISTS stream_pending_deadline_idx
    ON stream_pending (claim_deadline);
