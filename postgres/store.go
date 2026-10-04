// Package postgres is an xoutbox.Store for PostgreSQL 12+ using database/sql
// (works with pgx/stdlib and lib/pq).
package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"strconv"
	"strings"
	"time"

	"github.com/Ali127Dev/xoutbox"
	"github.com/Ali127Dev/xoutbox/internal/sqlstore"
)

//go:embed schema.sql
var schema string

var dialect = sqlstore.Dialect{
	Rebind:  rebind,
	Now:     "NOW()",
	Dur:     "(?::double precision * INTERVAL '1 millisecond')",
	DurArg:  func(d time.Duration) any { return float64(d) / float64(time.Millisecond) },
	Created: "(EXTRACT(EPOCH FROM created_at) * 1000000)::bigint",
	Age:     "EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))::double precision",
	Delete: `DELETE FROM {t} WHERE seq IN (SELECT seq FROM {t}
		WHERE status = 'published' AND published_at < NOW() - {dur} LIMIT 1000)`,
	Schema: schema,
}

// Option configures the store.
type Option = sqlstore.Option

// WithTable sets the table name, optionally schema-qualified (default "outbox").
func WithTable(name string) Option { return sqlstore.WithTable(name) }

// WithOrdering enables strict per-key ordering: an event is not claimed while an
// older event with the same Key is pending or in flight (head-of-line blocking).
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

// Claim atomically leases due rows in a single round trip using FOR UPDATE SKIP LOCKED.
func (s *Store) Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]xoutbox.Event, error) {
	q := s.Q(`WITH c AS (
		SELECT o.seq AS claim_seq FROM {t} o WHERE ` + s.Filter() + `
		ORDER BY o.seq LIMIT ? FOR UPDATE SKIP LOCKED)
	UPDATE {t} SET status = 'processing', locked_by = ?, locked_until = {now} + {dur}, attempts = attempts + 1
	FROM c WHERE seq = c.claim_seq
	RETURNING ` + sqlstore.Cols)
	rows, err := s.DB.QueryContext(ctx, q, limit, owner, dialect.DurArg(lease))
	if err != nil {
		return nil, err
	}
	return sqlstore.Scan(rows)
}

func rebind(q string) string {
	var sb strings.Builder
	n := 0
	for i := 0; i < len(q); i++ {
		if q[i] == '?' {
			n++
			sb.WriteString("$" + strconv.Itoa(n))
			continue
		}
		sb.WriteByte(q[i])
	}
	return sb.String()
}
