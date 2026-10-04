CREATE TABLE IF NOT EXISTS {t} (
    seq           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id            TEXT        NOT NULL UNIQUE,
    topic         TEXT        NOT NULL,
    partition_key TEXT        NOT NULL DEFAULT '',
    event_type    TEXT        NOT NULL DEFAULT '',
    payload       BYTEA       NOT NULL,
    headers       JSONB,
    status        TEXT        NOT NULL DEFAULT 'pending',
    attempts      INT         NOT NULL DEFAULT 0,
    max_attempts  INT         NOT NULL DEFAULT 10,
    last_error    TEXT,
    locked_by     TEXT,
    locked_until  TIMESTAMPTZ,
    available_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS {n}_claim_idx ON {t} (seq) WHERE status IN ('pending', 'processing');
CREATE INDEX IF NOT EXISTS {n}_key_idx ON {t} (partition_key, seq) WHERE status IN ('pending', 'processing');
CREATE INDEX IF NOT EXISTS {n}_published_idx ON {t} (published_at) WHERE status = 'published';
