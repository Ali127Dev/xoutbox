// Package mysql is an xoutbox.Store for MySQL 8.0+ / MariaDB 10.6+ using database/sql.
package mysql

import (
	"context"
	"database/sql"
	_ "embed"
	"time"

	"github.com/Ali127Dev/xoutbox"
	"github.com/Ali127Dev/xoutbox/internal/sqlstore"
)

//go:embed schema.sql
var schema string

var dialect = sqlstore.Dialect{
	Rebind:  func(q string) string { return q },
	Now:     "CURRENT_TIMESTAMP(6)",
	Dur:     "INTERVAL ? MICROSECOND",
	DurArg:  func(d time.Duration) any { return d.Microseconds() },
	Created: "CAST(UNIX_TIMESTAMP(created_at) * 1000000 AS SIGNED)",
	Age:     "TIMESTAMPDIFF(MICROSECOND, MIN(created_at), CURRENT_TIMESTAMP(6)) / 1000000.0",
	Delete: `DELETE FROM {t} WHERE status = 'published'
		AND published_at < CURRENT_TIMESTAMP(6) - {dur} ORDER BY published_at LIMIT 1000`,
	Schema: schema,
}

// Option configures the store.
type Option = sqlstore.Option

// WithTable sets the table name (default "outbox").
func WithTable(name string) Option { return sqlstore.WithTable(name) }

// WithOrdering enables strict per-key ordering (head-of-line blocking per Key).
func WithOrdering() Option { return sqlstore.WithOrdering() }

// WithMaxAttempts sets the default attempt limit for new events (default 10).
func WithMaxAttempts(n int) Option { return sqlstore.WithMaxAttempts(n) }

// Store implements xoutbox.Store and xoutbox.Cleaner.
type Store struct{ *sqlstore.Base }

var _ interface {
	xoutbox.Store
	xoutbox.Cleaner
} = (*Store)(nil)

func New(db *sql.DB, opts ...Option) *Store { return &Store{sqlstore.New(db, dialect, opts)} }

// Claim leases due rows inside a short transaction using FOR UPDATE SKIP LOCKED.
func (s *Store) Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]xoutbox.Event, error) {
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	rows, err := tx.QueryContext(ctx, s.Q(`SELECT o.seq FROM {t} o WHERE `+s.Filter()+`
		ORDER BY o.seq LIMIT ? FOR UPDATE SKIP LOCKED`), limit)
	if err != nil {
		return nil, err
	}
	var seqs []any
	for rows.Next() {
		var seq uint64
		if err := rows.Scan(&seq); err != nil {
			rows.Close()
			return nil, err
		}
		seqs = append(seqs, seq)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(seqs) == 0 {
		return nil, tx.Commit()
	}

	in := sqlstore.Placeholders(len(seqs))
	args := append([]any{owner, dialect.DurArg(lease)}, seqs...)
	if _, err := tx.ExecContext(ctx, s.Q(`UPDATE {t} SET status = 'processing', locked_by = ?,
		locked_until = {now} + {dur}, attempts = attempts + 1 WHERE seq IN (`+in+`)`), args...); err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx, s.Q(`SELECT `+sqlstore.Cols+` FROM {t} WHERE seq IN (`+in+`)`), seqs...)
	if err != nil {
		return nil, err
	}
	events, err := sqlstore.Scan(rows)
	if err != nil {
		return nil, err
	}
	return events, tx.Commit()
}
