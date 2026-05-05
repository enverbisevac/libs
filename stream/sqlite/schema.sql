CREATE TABLE IF NOT EXISTS stream_messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    stream      TEXT NOT NULL,
    payload     BLOB NOT NULL,
    headers     TEXT NOT NULL DEFAULT '{}',
    created_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS stream_messages_stream_id_idx
    ON stream_messages (stream, id);

CREATE INDEX IF NOT EXISTS stream_messages_stream_created_idx
    ON stream_messages (stream, created_at);

CREATE TABLE IF NOT EXISTS stream_groups (
    stream     TEXT NOT NULL,
    group_name TEXT NOT NULL,
    last_id    INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (stream, group_name)
);

CREATE TABLE IF NOT EXISTS stream_pending (
    stream         TEXT NOT NULL,
    group_name     TEXT NOT NULL,
    msg_id         INTEGER NOT NULL,
    consumer       TEXT NOT NULL,
    claim_deadline INTEGER NOT NULL,
    attempts       INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (stream, group_name, msg_id)
);

CREATE INDEX IF NOT EXISTS stream_pending_deadline_idx
    ON stream_pending (claim_deadline);
