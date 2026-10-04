package xoutbox_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Ali127Dev/xoutbox"
)

type row struct {
	e      xoutbox.Event
	status xoutbox.Status
	owner  string
	until  time.Time
	avail  time.Time
}

type memStore struct {
	mu   sync.Mutex
	rows []*row
}

func (m *memStore) add(events ...xoutbox.Event) {
	for _, e := range events {
		if e.MaxAttempts == 0 {
			e.MaxAttempts = 10
		}
		m.rows = append(m.rows, &row{e: e, status: xoutbox.StatusPending})
	}
}

func (m *memStore) Claim(_ context.Context, owner string, limit int, lease time.Duration) ([]xoutbox.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	var out []xoutbox.Event
	for _, r := range m.rows {
		if len(out) == limit {
			break
		}
		due := r.status == xoutbox.StatusPending && !r.avail.After(now) ||
			r.status == xoutbox.StatusProcessing && r.until.Before(now) && r.e.Attempts < r.e.MaxAttempts
		if !due {
			continue
		}
		r.status, r.owner, r.until = xoutbox.StatusProcessing, owner, now.Add(lease)
		r.e.Attempts++
		out = append(out, r.e)
	}
	return out, nil
}

func (m *memStore) MarkPublished(_ context.Context, ids []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.rows {
		if slices.Contains(ids, r.e.ID) {
			r.status = xoutbox.StatusPublished
		}
	}
	return nil
}

func (m *memStore) MarkFailed(_ context.Context, owner string, f xoutbox.Failure) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.rows {
		if r.e.ID == f.ID && r.owner == owner {
			r.status = xoutbox.StatusPending
			if f.Dead {
				r.status = xoutbox.StatusDead
			}
			r.e.Attempts, r.avail = f.Attempts, time.Now().Add(f.Delay)
		}
	}
	return nil
}

func (m *memStore) count(s xoutbox.Status) (n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.rows {
		if r.status == s {
			n++
		}
	}
	return n
}

var noDelay = xoutbox.WithBackoff(func(int) time.Duration { return 0 })

func drain(t *testing.T, w *xoutbox.Worker) {
	t.Helper()
	for range 100 {
		n, err := w.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
	}
	t.Fatal("worker did not drain")
}

func TestPublishesAll(t *testing.T) {
	s := &memStore{}
	for range 25 {
		s.add(xoutbox.NewEvent("t", "x", []byte("p")))
	}
	var mu sync.Mutex
	got := 0
	pub := xoutbox.PublisherFunc(func(context.Context, xoutbox.Event) error { mu.Lock(); got++; mu.Unlock(); return nil })
	drain(t, xoutbox.NewWorker(s, pub, xoutbox.WithBatchSize(10)))
	if got != 25 || s.count(xoutbox.StatusPublished) != 25 {
		t.Fatalf("published %d, rows %d", got, s.count(xoutbox.StatusPublished))
	}
}

func TestRetryThenDead(t *testing.T) {
	s := &memStore{}
	s.add(xoutbox.NewEvent("t", "x", nil).WithMaxAttempts(3))
	calls, deadHooks := 0, 0
	pub := xoutbox.PublisherFunc(func(context.Context, xoutbox.Event) error { calls++; return errors.New("boom") })
	hooks := xoutbox.WithHooks(xoutbox.Hooks{OnFailed: func(_ xoutbox.Event, _ error, dead bool) {
		if dead {
			deadHooks++
		}
	}})
	drain(t, xoutbox.NewWorker(s, pub, noDelay, hooks))
	if calls != 3 || deadHooks != 1 || s.count(xoutbox.StatusDead) != 1 {
		t.Fatalf("calls=%d deadHooks=%d dead=%d", calls, deadHooks, s.count(xoutbox.StatusDead))
	}
}

func TestPermanentGoesDead(t *testing.T) {
	s := &memStore{}
	s.add(xoutbox.NewEvent("t", "x", nil))
	pub := xoutbox.PublisherFunc(func(context.Context, xoutbox.Event) error { return xoutbox.Permanent(errors.New("bad")) })
	drain(t, xoutbox.NewWorker(s, pub, noDelay))
	if s.count(xoutbox.StatusDead) != 1 {
		t.Fatal("expected dead")
	}
}

func TestKeyOrderingPreservedOnFailure(t *testing.T) {
	s := &memStore{}
	for _, id := range []string{"a1", "a2", "a3"} {
		e := xoutbox.NewEvent("t", "x", nil).WithKey("a")
		e.ID = id
		s.add(e)
	}
	var order []string
	failed := false
	pub := xoutbox.PublisherFunc(func(_ context.Context, e xoutbox.Event) error {
		if e.ID == "a1" && !failed {
			failed = true
			return errors.New("transient")
		}
		order = append(order, e.ID)
		return nil
	})
	drain(t, xoutbox.NewWorker(s, pub, noDelay))
	if !slices.Equal(order, []string{"a1", "a2", "a3"}) {
		t.Fatalf("order %v", order)
	}
	for _, r := range s.rows {
		if r.e.ID != "a1" && r.e.Attempts != 1 {
			t.Fatalf("blocked event %s consumed an attempt: %d", r.e.ID, r.e.Attempts)
		}
	}
}

type batchPub struct{ calls int }

func (b *batchPub) Publish(context.Context, xoutbox.Event) error { return errors.New("unused") }
func (b *batchPub) PublishBatch(_ context.Context, ev []xoutbox.Event) []error {
	b.calls++
	errs := make([]error, len(ev))
	errs[0] = xoutbox.Permanent(errors.New("first is bad"))
	return errs
}

func TestBatchPublisher(t *testing.T) {
	s := &memStore{}
	for range 5 {
		s.add(xoutbox.NewEvent("t", "x", nil))
	}
	b := &batchPub{}
	if _, err := xoutbox.NewWorker(s, b).RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b.calls != 1 || s.count(xoutbox.StatusPublished) != 4 || s.count(xoutbox.StatusDead) != 1 {
		t.Fatalf("calls=%d published=%d", b.calls, s.count(xoutbox.StatusPublished))
	}
}

func TestPanicRecovered(t *testing.T) {
	s := &memStore{}
	s.add(xoutbox.NewEvent("t", "x", nil))
	pub := xoutbox.PublisherFunc(func(context.Context, xoutbox.Event) error { panic("oops") })
	if _, err := xoutbox.NewWorker(s, pub, noDelay).RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.count(xoutbox.StatusPending) != 1 {
		t.Fatal("panicking event should be retried")
	}
}

func TestRunGracefulShutdown(t *testing.T) {
	s := &memStore{}
	s.add(xoutbox.NewEvent("t", "x", nil))
	ctx, cancel := context.WithCancel(context.Background())
	pub := xoutbox.PublisherFunc(func(context.Context, xoutbox.Event) error { cancel(); return nil })
	done := make(chan error)
	go func() { done <- xoutbox.NewWorker(s, pub).Run(ctx) }()
	select {
	case err := <-done:
		if err != nil || s.count(xoutbox.StatusPublished) != 1 {
			t.Fatalf("err=%v published=%d", err, s.count(xoutbox.StatusPublished))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestNewIDIsV7(t *testing.T) {
	a, b := xoutbox.NewID(), xoutbox.NewID()
	if len(a) != 36 || a[14] != '7' || a == b {
		t.Fatalf("bad ids %s %s", a, b)
	}
}
