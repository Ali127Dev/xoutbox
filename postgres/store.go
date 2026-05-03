package postgres

import (
	"context"
	"database/sql"

	"github.com/Ali127Dev/xoutbox"
)

type Store[T comparable] struct {
	db *sql.DB
}

func New[T comparable](db *sql.DB) *Store[T] {
	return &Store[T]{db: db}
}

func (s *Store[T]) InsertEvent(ctx context.Context, event xoutbox.Event[T]) error {
	const q = `
		INSERT INTO outbox (id, event_type, payload, status, retry_count, max_retries, created_at, updated_at)
		VALUES ($1, $2, $3, 'pending', 0, $4, NOW(), NOW())
	`
	_, err := s.db.ExecContext(ctx, q,
		event.ID,
		event.EventType,
		event.Payload,
		event.MaxRetries,
	)
	return err
}

func (s *Store[T]) FetchPending(ctx context.Context, limit int) ([]xoutbox.Event[T], error) {
	const q = `
		SELECT id, event_type, payload, retry_count, max_retries, created_at, published_at
		FROM outbox
		WHERE status = 'pending'
		  AND retry_count < max_retries
		ORDER BY created_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT $1
	`

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	rows, err := tx.QueryContext(ctx, q, limit)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	defer rows.Close()

	events := make([]xoutbox.Event[T], 0, limit)

	for rows.Next() {
		var e xoutbox.Event[T]

		err := rows.Scan(
			&e.ID,
			&e.EventType,
			&e.Payload,
			&e.RetryCount,
			&e.MaxRetries,
			&e.CreatedAt,
			&e.PublishedAt,
		)
		if err != nil {
			_ = tx.Rollback()
			return nil, err
		}

		e.Status = xoutbox.StatusPending
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return events, nil
}

func (s *Store[T]) MarkPublished(ctx context.Context, id T) error {
	const q = `
		UPDATE outbox
		SET status = 'published', published_at = NOW(), updated_at = NOW()
		WHERE id = $1
	`
	_, err := s.db.ExecContext(ctx, q, id)
	return err
}

func (s *Store[T]) MarkFailed(ctx context.Context, id T, retryCount int) error {
	const q = `
		UPDATE outbox
		SET retry_count = $2,
		    status = CASE WHEN $2 >= max_retries THEN 'dead' ELSE 'pending' END,
		    updated_at = NOW()
		WHERE id = $1
	`
	_, err := s.db.ExecContext(ctx, q, id, retryCount)
	return err
}
