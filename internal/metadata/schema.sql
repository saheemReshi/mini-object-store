-- Controller metadata schema (version 1). Every statement must be idempotent:
-- it runs on every startup against both new and existing databases.

CREATE TABLE IF NOT EXISTS objects (
    id              TEXT    PRIMARY KEY,
    name            TEXT    NOT NULL,
    size            INTEGER NOT NULL,
    content_type    TEXT    NOT NULL,
    created_at      TEXT    NOT NULL, -- RFC 3339, nanosecond precision, UTC
    modified_at     TEXT    NOT NULL, -- RFC 3339, nanosecond precision, UTC
    checksum        TEXT    NOT NULL, -- hex SHA-256 of the whole object
    chunk_count     INTEGER NOT NULL,
    custom_metadata TEXT    NOT NULL DEFAULT '{}', -- JSON object of strings
    deleting        INTEGER NOT NULL DEFAULT 0 CHECK (deleting IN (0, 1))
);

-- Speeds up ListObjects, which hides tombstones.
CREATE INDEX IF NOT EXISTS objects_live ON objects (deleting, id);

CREATE TABLE IF NOT EXISTS chunks (
    object_id   TEXT    NOT NULL REFERENCES objects (id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    id          TEXT    NOT NULL,
    size        INTEGER NOT NULL,
    checksum    TEXT    NOT NULL, -- hex SHA-256 of this chunk
    PRIMARY KEY (object_id, chunk_index),
    UNIQUE (object_id, id)
);

CREATE TABLE IF NOT EXISTS replicas (
    object_id TEXT    NOT NULL,
    chunk_id  TEXT    NOT NULL,
    slot      INTEGER NOT NULL, -- preserves replica order
    worker_id TEXT    NOT NULL,
    PRIMARY KEY (object_id, chunk_id, slot),
    UNIQUE (object_id, chunk_id, worker_id),
    FOREIGN KEY (object_id, chunk_id)
        REFERENCES chunks (object_id, id) ON DELETE CASCADE
);
