// Package storetest is a conformance suite for SQL stores.
package storetest

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ali127Dev/xoutbox"
	"github.com/Ali127Dev/xoutbox/internal/sqlstore"
)

type Store interface {
	xoutbox.Store
	xoutbox.Cleaner
	Insert(ctx context.Context, ex xoutbox.Execer, events ...xoutbox.Event) error
	Requeue(ctx context.Context, ids ...string) (int64, error)
	Stats(ctx context.Context) (xoutbox.Stats, error)
	Migrate(ctx context.Context) error
}

var seq atomic.Int64

func Run(t *testing.T, db *sql.DB, newStore func(...sqlstore.Option) Store) {
	ctx := context.Background()
	mk := func(t *testing.T, opts ...sqlstore.Option) Store {
		name := fmt.Sprintf("outbox_test_%d_%d", time.Now().UnixNano()%1e6, seq.Add(1))
		s := newStore(append(opts, sqlstore.WithTable(name))...)
		for range 2 { // idempotent
			if err := s.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + name) })
		return s
	}
	must := func(t *testing.T, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Run("transactional insert and claim", func(t *testing.T) {
		s := mk(t)
		tx, _ := db.Begin()
		must(t, s.Insert(ctx, tx, xoutbox.NewEvent("t", "x", []byte("rolled back"))))
		must(t, tx.Rollback())

		tx, _ = db.Begin()
		e := xoutbox.NewEvent("orders", "order.created", []byte("hi")).WithKey("k").WithHeader("h", "v")
		must(t, s.Insert(ctx, tx, e, xoutbox.NewEvent("orders", "y", nil)))
		must(t, tx.Commit())

		got, err := s.Claim(ctx, "w1", 10, time.Minute)
		must(t, err)
		if len(got) != 2 || got[0].ID != e.ID || got[0].Headers["h"] != "v" || got[0].Key != "k" ||
			string(got[0].Payload) != "hi" || got[0].Attempts != 1 || got[0].MaxAttempts != 10 ||
			time.Since(got[0].CreatedAt) > time.Minute {
			t.Fatalf("unexpected claim %+v", got)
		}
		again, err := s.Claim(ctx, "w2", 10, time.Minute)
		must(t, err)
		if len(again) != 0 {
			t.Fatal("leased events claimed twice")
		}

		must(t, s.MarkFailed(ctx, "intruder", xoutbox.Failure{ID: e.ID, Attempts: 1}))
		must(t, s.MarkFailed(ctx, "w1", xoutbox.Failure{ID: e.ID, Attempts: 1, Err: "x"}))
		must(t, s.MarkPublished(ctx, []string{got[1].ID}))
		got, err = s.Claim(ctx, "w2", 10, time.Minute)
		must(t, err)
		if len(got) != 1 || got[0].ID != e.ID || got[0].Attempts != 2 {
			t.Fatalf("retry claim %+v", got)
		}
	})

	t.Run("lease expiry", func(t *testing.T) {
		s := mk(t)
		must(t, s.Insert(ctx, db, xoutbox.NewEvent("t", "x", nil)))
		got, _ := s.Claim(ctx, "w1", 1, 10*time.Millisecond)
		time.Sleep(50 * time.Millisecond)
		again, err := s.Claim(ctx, "w2", 1, time.Minute)
		must(t, err)
		if len(got) != 1 || len(again) != 1 || again[0].Attempts != 2 {
			t.Fatal("expired lease not reclaimed")
		}
	})

	t.Run("delay", func(t *testing.T) {
		s := mk(t)
		must(t, s.Insert(ctx, db, xoutbox.NewEvent("t", "x", nil).WithDelay(time.Hour)))
		got, err := s.Claim(ctx, "w", 10, time.Minute)
		must(t, err)
		if len(got) != 0 {
			t.Fatal("delayed event claimed early")
		}
	})

	t.Run("ordering", func(t *testing.T) {
		s := mk(t, sqlstore.WithOrdering())
		a, b := xoutbox.NewEvent("t", "1", nil).WithKey("k"), xoutbox.NewEvent("t", "2", nil).WithKey("k")
		must(t, s.Insert(ctx, db, a, b, xoutbox.NewEvent("t", "3", nil)))
		got, err := s.Claim(ctx, "w", 10, time.Minute)
		must(t, err)
		if len(got) != 2 || got[0].ID != a.ID {
			t.Fatalf("ordered claim %+v", got)
		}
		must(t, s.MarkPublished(ctx, []string{a.ID}))
		got, _ = s.Claim(ctx, "w", 10, time.Minute)
		if len(got) != 1 || got[0].ID != b.ID {
			t.Fatalf("second ordered claim %+v", got)
		}
	})

	t.Run("dead requeue stats cleanup", func(t *testing.T) {
		s := mk(t)
		e := xoutbox.NewEvent("t", "x", nil)
		must(t, s.Insert(ctx, db, e, xoutbox.NewEvent("t", "y", nil)))
		got, _ := s.Claim(ctx, "w", 10, time.Minute)
		must(t, s.MarkFailed(ctx, "w", xoutbox.Failure{ID: e.ID, Attempts: 1, Dead: true, Err: "bad"}))
		must(t, s.MarkPublished(ctx, []string{got[1].ID}))
		st, err := s.Stats(ctx)
		must(t, err)
		if st.Dead != 1 || st.Pending != 0 {
			t.Fatalf("stats %+v", st)
		}
		n, err := s.Requeue(ctx)
		must(t, err)
		if n != 1 {
			t.Fatal("requeue")
		}
		time.Sleep(5 * time.Millisecond)
		deleted, err := s.Cleanup(ctx, time.Millisecond)
		must(t, err)
		st, _ = s.Stats(ctx)
		if deleted != 1 || st.Pending != 1 {
			t.Fatalf("deleted=%d stats=%+v", deleted, st)
		}
	})

	t.Run("reaper", func(t *testing.T) {
		s := mk(t)
		must(t, s.Insert(ctx, db, xoutbox.NewEvent("t", "x", nil).WithMaxAttempts(1)))
		_, _ = s.Claim(ctx, "w", 1, time.Millisecond)
		time.Sleep(20 * time.Millisecond)
		_, err := s.Cleanup(ctx, 0)
		must(t, err)
		if st, _ := s.Stats(ctx); st.Dead != 1 {
			t.Fatalf("poison event not reaped: %+v", st)
		}
	})

	t.Run("concurrent workers deliver each event once", func(t *testing.T) {
		s := mk(t)
		const total = 300
		events := make([]xoutbox.Event, total)
		for i := range events {
			events[i] = xoutbox.NewEvent("t", "x", nil)
		}
		must(t, s.Insert(ctx, db, events...))
		var mu sync.Mutex
		seen := map[string]int{}
		pub := xoutbox.PublisherFunc(func(_ context.Context, e xoutbox.Event) error {
			mu.Lock()
			seen[e.ID]++
			mu.Unlock()
			return nil
		})
		var wg sync.WaitGroup
		for i := range 4 {
			w := xoutbox.NewWorker(s, pub, xoutbox.WithBatchSize(25), xoutbox.WithWorkerID(fmt.Sprint("w", i)))
			wg.Go(func() {
				for {
					n, err := w.RunOnce(ctx)
					if err != nil {
						t.Error(err)
						return
					}
					if n == 0 {
						return
					}
				}
			})
		}
		wg.Wait()
		if len(seen) != total {
			t.Fatalf("delivered %d/%d", len(seen), total)
		}
		for id, n := range seen {
			if n != 1 {
				t.Fatalf("%s delivered %d times", id, n)
			}
		}
	})
}
