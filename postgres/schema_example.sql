CREATE TABLE outbox (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload BYTEA NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 5,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP NULL
);