// Package sqlstore holds the dialect-independent SQL store implementation.
package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Ali127Dev/xoutbox"
)

// Dialect captures the SQL differences between databases.
type Dialect struct {
	Rebind  func(string) string
	Now     string                  // current timestamp expression
	Dur     string                  // duration expression with exactly one placeholder
	DurArg  func(time.Duration) any // value bound to Dur
	Created string                  // created_at as unix microseconds
	Age     string                  // seconds since MIN(created_at)
	Delete  string                  // deletes up to 1000 published rows older than {dur}
	Schema  string
}

// Base implements everything except Claim.
type Base struct {
	DB          *sql.DB
	Table       string
	Ordered     bool
	MaxAttempts int
	D           Dialect
}

// Option configures a Base.
type Option func(*Base)

var ident = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

func WithTable(name string) Option {
	if !ident.MatchString(name) {
		panic("xoutbox: invalid table name " + strconv.Quote(name))
	}
	return func(b *Base) { b.Table = name }
}
func WithOrdering() Option         { return func(b *Base) { b.Ordered = true } }
func WithMaxAttempts(n int) Option { return func(b *Base) { b.MaxAttempts = n } }

func New(db *sql.DB, d Dialect, opts []Option) *Base {
	b := &Base{DB: db, Table: "outbox", MaxAttempts: 10, D: d}
	for _, o := range opts {
		o(b)
	}
	return b
}

// Q expands {t} {n} {now} {dur} {created} and rebinds placeholders.
func (b *Base) Q(s string) string {
	name := b.Table[strings.LastIndexByte(b.Table, '.')+1:]
	return b.D.Rebind(strings.NewReplacer(
		"{t}", b.Table, "{n}", name, "{now}", b.D.Now, "{dur}", b.D.Dur, "{created}", b.D.Created,
	).Replace(s))
}

// Cols is the column list returned by claims, matching Scan.
const Cols = "seq, id, topic, partition_key, event_type, payload, headers, attempts, max_attempts, {created}"

// Filter is the WHERE condition selecting claimable rows (table alias o).
func (b *Base) Filter() string {
	f := `((o.status = 'pending' AND o.available_at <= {now})
	   OR (o.status = 'processing' AND o.locked_until < {now} AND o.attempts < o.max_attempts))`
	if b.Ordered {
		f += ` AND (o.partition_key = '' OR NOT EXISTS (
		SELECT 1 FROM {t} p WHERE p.partition_key = o.partition_key AND p.seq < o.seq
		AND p.status IN ('pending', 'processing')))`
	}
	return f
}

// Scan reads Cols rows sorted by seq.
func Scan(rows *sql.Rows) ([]xoutbox.Event, error) {
	defer rows.Close()
	type row struct {
		seq int64
		e   xoutbox.Event
	}
	var out []row
	for rows.Next() {
		var (
			r       row
			headers sql.NullString
			created int64
		)
		if err := rows.Scan(&r.seq, &r.e.ID, &r.e.Topic, &r.e.Key, &r.e.Type, &r.e.Payload,
			&headers, &r.e.Attempts, &r.e.MaxAttempts, &created); err != nil {
			return nil, err
		}
		if headers.Valid && headers.String != "" {
			if err := json.Unmarshal([]byte(headers.String), &r.e.Headers); err != nil {
				return nil, fmt.Errorf("decode headers of %s: %w", r.e.ID, err)
			}
		}
		r.e.CreatedAt = time.UnixMicro(created)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(out, func(a, b row) int { return int(a.seq - b.seq) })
	events := make([]xoutbox.Event, len(out))
	for i, r := range out {
		events[i] = r.e
	}
	return events, nil
}

// Placeholders returns "?, ?, ..." with n entries.
func Placeholders(n int) string { return strings.TrimSuffix(strings.Repeat("?, ", n), ", ") }

const chunk = 1000

// Insert writes events using ex, which should be your business transaction.
func (b *Base) Insert(ctx context.Context, ex xoutbox.Execer, events ...xoutbox.Event) error {
	for start := 0; start < len(events); start += chunk {
		part := events[start:min(start+chunk, len(events))]
		var sb strings.Builder
		sb.WriteString("INSERT INTO {t} (id, topic, partition_key, event_type, payload, headers, max_attempts, available_at) VALUES ")
		args := make([]any, 0, len(part)*8)
		for i, e := range part {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(?, ?, ?, ?, ?, ?, ?, {now} + {dur})")
			if e.ID == "" {
				e.ID = xoutbox.NewID()
			}
			if e.Topic == "" {
				return fmt.Errorf("xoutbox: event %s has no topic", e.ID)
			}
			if e.Payload == nil {
				e.Payload = []byte{}
			}
			if e.MaxAttempts <= 0 {
				e.MaxAttempts = b.MaxAttempts
			}
			var headers sql.NullString
			if len(e.Headers) > 0 {
				h, err := json.Marshal(e.Headers)
				if err != nil {
					return err
				}
				headers = sql.NullString{String: string(h), Valid: true}
			}
			var delay time.Duration
			if !e.AvailableAt.IsZero() {
				delay = max(time.Until(e.AvailableAt), 0)
			}
			args = append(args, e.ID, e.Topic, e.Key, e.Type, e.Payload, headers, e.MaxAttempts, b.D.DurArg(delay))
		}
		if _, err := ex.ExecContext(ctx, b.Q(sb.String()), args...); err != nil {
			return fmt.Errorf("xoutbox: insert: %w", err)
		}
	}
	return nil
}

func (b *Base) MarkPublished(ctx context.Context, ids []string) error {
	for start := 0; start < len(ids); start += chunk {
		part := ids[start:min(start+chunk, len(ids))]
		q := b.Q(`UPDATE {t} SET status = 'published', published_at = {now}, locked_by = NULL,
			locked_until = NULL, last_error = NULL WHERE id IN (` + Placeholders(len(part)) + `)`)
		if _, err := b.DB.ExecContext(ctx, q, toAny(part)...); err != nil {
			return err
		}
	}
	return nil
}

func (b *Base) MarkFailed(ctx context.Context, owner string, f xoutbox.Failure) error {
	status := xoutbox.StatusPending
	if f.Dead {
		status = xoutbox.StatusDead
	}
	msg := f.Err
	if len(msg) > 2000 {
		msg = msg[:2000]
	}
	_, err := b.DB.ExecContext(ctx, b.Q(`UPDATE {t} SET status = ?, attempts = ?, last_error = ?,
		available_at = {now} + {dur}, locked_by = NULL, locked_until = NULL
		WHERE id = ? AND locked_by = ?`),
		string(status), f.Attempts, msg, b.D.DurArg(f.Delay), f.ID, owner)
	return err
}

func (b *Base) Cleanup(ctx context.Context, retention time.Duration) (int64, error) {
	if _, err := b.DB.ExecContext(ctx, b.Q(`UPDATE {t} SET status = 'dead',
		last_error = 'lease expired after max attempts', locked_by = NULL, locked_until = NULL
		WHERE status = 'processing' AND locked_until < {now} AND attempts >= max_attempts`)); err != nil {
		return 0, err
	}
	if retention <= 0 {
		return 0, nil
	}
	var total int64
	q := b.Q(b.D.Delete)
	for {
		res, err := b.DB.ExecContext(ctx, q, b.D.DurArg(retention))
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
		if n < chunk {
			return total, nil
		}
	}
}

// Requeue moves dead events back to pending with a fresh attempt budget.
// With no ids, every dead event is requeued.
func (b *Base) Requeue(ctx context.Context, ids ...string) (int64, error) {
	q := `UPDATE {t} SET status = 'pending', attempts = 0, last_error = NULL, available_at = {now} WHERE status = 'dead'`
	if len(ids) > 0 {
		q += ` AND id IN (` + Placeholders(len(ids)) + `)`
	}
	res, err := b.DB.ExecContext(ctx, b.Q(q), toAny(ids)...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Stats returns backlog counters, cheap enough to scrape as metrics.
func (b *Base) Stats(ctx context.Context) (xoutbox.Stats, error) {
	var s xoutbox.Stats
	rows, err := b.DB.QueryContext(ctx, b.Q(`SELECT status, COUNT(*), `+b.D.Age+` FROM {t}
		WHERE status IN ('pending', 'processing', 'dead') GROUP BY status`))
	if err != nil {
		return s, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			st  string
			n   int64
			age sql.NullFloat64
		)
		if err := rows.Scan(&st, &n, &age); err != nil {
			return s, err
		}
		switch xoutbox.Status(st) {
		case xoutbox.StatusPending:
			s.Pending = n
			s.OldestPending = time.Duration(age.Float64 * float64(time.Second))
		case xoutbox.StatusProcessing:
			s.Processing = n
		case xoutbox.StatusDead:
			s.Dead = n
		}
	}
	return s, rows.Err()
}

// Migrate creates the table and indexes if they do not exist.
func (b *Base) Migrate(ctx context.Context) error {
	for stmt := range strings.SplitSeq(b.D.Schema, ";") {
		if stmt = strings.TrimSpace(stmt); stmt == "" {
			continue
		}
		if _, err := b.DB.ExecContext(ctx, b.Q(stmt)); err != nil {
			return fmt.Errorf("xoutbox: migrate: %w", err)
		}
	}
	return nil
}

// SchemaSQL returns the DDL for the configured table.
func (b *Base) SchemaSQL() string { return b.Q(b.D.Schema) }

func toAny[T any](s []T) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}
