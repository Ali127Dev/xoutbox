package xoutbox

import (
	"context"
	"database/sql"
	"time"
)

// Store persists outbox events. Implementations must be safe for concurrent use
// by many workers across many processes.
type Store interface {
	// Claim leases up to limit due events to owner for the lease duration,
	// increments their attempt counter and returns them ordered by insertion.
	// Events whose lease expired (crashed worker) are claimable again.
	Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]Event, error)
	// MarkPublished marks events as delivered.
	MarkPublished(ctx context.Context, ids []string) error
	// MarkFailed records a failed attempt. It must be a no-op if owner no longer holds the lease.
	MarkFailed(ctx context.Context, owner string, f Failure) error
}

// Failure describes a failed delivery attempt.
type Failure struct {
	ID       string
	Attempts int           // attempt counter to persist
	Delay    time.Duration // time until the event becomes claimable again
	Err      string
	Dead     bool // move to dead-letter state instead of retrying
}

// Cleaner is implemented by stores that support housekeeping. Cleanup dead-letters
// events whose lease expired after exhausting their attempts and, if retention > 0,
// deletes published events older than retention.
type Cleaner interface {
	Cleanup(ctx context.Context, retention time.Duration) (deleted int64, err error)
}

// Stats is a snapshot of the outbox backlog.
type Stats struct {
	Pending       int64
	Processing    int64
	Dead          int64
	OldestPending time.Duration
}

// Execer is satisfied by *sql.DB, *sql.Tx and *sql.Conn. Pass your business
// transaction so the event is committed atomically with your data.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}
