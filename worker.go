package xoutbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Worker relays events from a Store to a Publisher with at-least-once semantics.
// Run any number of workers across any number of processes; leases prevent
// double delivery under normal operation and recover events from crashed workers.
type Worker struct {
	store Store
	pub   Publisher
	batch BatchPublisher
	o     options
	wake  chan struct{}
}

// NewWorker creates a worker. If pub implements BatchPublisher, batches are
// published in one call.
func NewWorker(store Store, pub Publisher, opts ...Option) *Worker {
	w := &Worker{store: store, pub: pub, o: newOptions(opts), wake: make(chan struct{}, 1)}
	if bp, ok := pub.(BatchPublisher); ok {
		w.batch = bp
	}
	return w
}

// ID returns the lease owner name of this worker.
func (w *Worker) ID() string { return w.o.owner }

// Notify wakes the worker immediately, e.g. right after committing a transaction
// that inserted events. It never blocks.
func (w *Worker) Notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Run polls until ctx is cancelled. The in-flight batch is always finished and
// committed before Run returns, so shutdown never loses state. Returns nil on
// graceful shutdown.
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	if c, ok := w.store.(Cleaner); ok && w.o.cleanupInterval > 0 {
		wg.Go(func() { w.cleanupLoop(ctx, c) })
	}

	work := context.WithoutCancel(ctx)
	timer := time.NewTimer(0)
	defer timer.Stop()
	streak := 0
	w.o.logger.Info("xoutbox: worker started", "worker", w.o.owner)

	for {
		select {
		case <-ctx.Done():
			w.o.logger.Info("xoutbox: worker stopped", "worker", w.o.owner)
			return nil
		case <-timer.C:
		case <-w.wake:
		}
		if ctx.Err() != nil {
			continue
		}

		n, err := w.RunOnce(work)
		next := w.o.pollInterval
		switch {
		case err != nil:
			streak++
			next = min(max(w.o.pollInterval, time.Second)<<min(streak, 5), 30*time.Second)
			w.o.logger.Error("xoutbox: batch failed", "worker", w.o.owner, "err", err, "retry_in", next)
		case n >= w.o.batchSize:
			streak, next = 0, 0
		default:
			streak = 0
		}
		timer.Reset(next)
	}
}

// RunOnce claims, publishes and commits a single batch. It returns the number of
// events claimed. Useful for tests, cron jobs and serverless environments.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	events, err := w.store.Claim(ctx, w.o.owner, w.o.batchSize, w.o.lease)
	if err != nil {
		w.storeError("claim", err)
		return 0, fmt.Errorf("xoutbox: claim: %w", err)
	}
	if len(events) == 0 {
		return 0, nil
	}
	var out outcome
	if w.batch != nil {
		out = w.publishBatch(ctx, events)
	} else {
		out = w.publishGrouped(ctx, events)
	}
	return len(events), w.commit(ctx, out)
}

type failed struct {
	e       Event
	err     error
	f       Failure
	blocked bool
}

type outcome struct {
	ok     []Event
	failed []failed
}

func (w *Worker) publishBatch(ctx context.Context, events []Event) outcome {
	ctx, cancel := context.WithTimeout(ctx, w.o.publishTimeout)
	defer cancel()

	errs := func() (errs []error) {
		defer func() {
			if r := recover(); r != nil {
				errs = nil
				w.o.logger.Error("xoutbox: publisher panic", "panic", r)
			}
		}()
		return w.batch.PublishBatch(ctx, events)
	}()
	if len(errs) != len(events) {
		err := fmt.Errorf("xoutbox: PublishBatch returned %d results for %d events", len(errs), len(events))
		errs = make([]error, len(events))
		for i := range errs {
			errs[i] = err
		}
	}

	var out outcome
	for i, e := range events {
		if errs[i] == nil {
			out.ok = append(out.ok, e)
		} else {
			out.failed = append(out.failed, failed{e: e, err: errs[i], f: w.failure(e, errs[i])})
		}
	}
	return out
}

// publishGrouped publishes events sharing a Key sequentially (preserving order)
// and different keys in parallel. When an event fails, the rest of its group is
// released without consuming an attempt.
func (w *Worker) publishGrouped(ctx context.Context, events []Event) outcome {
	var (
		out outcome
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, w.o.concurrency)
	)
	for _, g := range groupByKey(events) {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			for i, e := range g {
				err := w.publish(ctx, e)
				mu.Lock()
				if err == nil {
					out.ok = append(out.ok, e)
					mu.Unlock()
					continue
				}
				f := w.failure(e, err)
				out.failed = append(out.failed, failed{e: e, err: err, f: f})
				delay := f.Delay
				if f.Dead {
					delay = 0
				}
				for _, r := range g[i+1:] {
					out.failed = append(out.failed, failed{e: r, err: err, blocked: true, f: Failure{
						ID: r.ID, Attempts: r.Attempts - 1, Delay: delay, Err: "blocked by failed event " + e.ID,
					}})
				}
				mu.Unlock()
				return
			}
		})
	}
	wg.Wait()
	return out
}

func (w *Worker) publish(ctx context.Context, e Event) (err error) {
	ctx, cancel := context.WithTimeout(ctx, w.o.publishTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("xoutbox: publisher panic: %v", r)
		}
	}()
	return w.pub.Publish(ctx, e)
}

func (w *Worker) failure(e Event, err error) Failure {
	dead := IsPermanent(err) || (e.MaxAttempts > 0 && e.Attempts >= e.MaxAttempts)
	return Failure{ID: e.ID, Attempts: e.Attempts, Delay: w.o.backoff(e.Attempts), Err: err.Error(), Dead: dead}
}

func (w *Worker) commit(ctx context.Context, out outcome) error {
	var errs []error
	if len(out.ok) > 0 {
		ids := make([]string, len(out.ok))
		for i, e := range out.ok {
			ids[i] = e.ID
		}
		if err := retry(ctx, func() error { return w.store.MarkPublished(ctx, ids) }); err != nil {
			w.storeError("mark_published", err)
			errs = append(errs, fmt.Errorf("xoutbox: mark published: %w", err))
		} else if w.o.hooks.OnPublished != nil {
			for _, e := range out.ok {
				w.o.hooks.OnPublished(e)
			}
		}
	}
	for _, f := range out.failed {
		if err := retry(ctx, func() error { return w.store.MarkFailed(ctx, w.o.owner, f.f) }); err != nil {
			w.storeError("mark_failed", err)
			errs = append(errs, fmt.Errorf("xoutbox: mark failed %s: %w", f.e.ID, err))
			continue
		}
		if f.blocked {
			continue
		}
		lvl := slog.LevelWarn
		if f.f.Dead {
			lvl = slog.LevelError
		}
		w.o.logger.Log(ctx, lvl, "xoutbox: publish failed", "event_id", f.e.ID, "topic", f.e.Topic,
			"attempt", f.e.Attempts, "dead", f.f.Dead, "retry_in", f.f.Delay, "err", f.err)
		if w.o.hooks.OnFailed != nil {
			w.o.hooks.OnFailed(f.e, f.err, f.f.Dead)
		}
	}
	return errors.Join(errs...)
}

func (w *Worker) cleanupLoop(ctx context.Context, c Cleaner) {
	t := time.NewTicker(w.o.cleanupInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := c.Cleanup(ctx, w.o.retention)
			if err != nil {
				if ctx.Err() == nil {
					w.storeError("cleanup", err)
					w.o.logger.Error("xoutbox: cleanup failed", "err", err)
				}
			} else if n > 0 {
				w.o.logger.Info("xoutbox: cleanup", "deleted", n)
			}
		}
	}
}

func (w *Worker) storeError(op string, err error) {
	if w.o.hooks.OnStoreError != nil {
		w.o.hooks.OnStoreError(op, err)
	}
}

func groupByKey(events []Event) [][]Event {
	groups := make([][]Event, 0, len(events))
	idx := make(map[string]int)
	for _, e := range events {
		if e.Key != "" {
			if i, ok := idx[e.Key]; ok {
				groups[i] = append(groups[i], e)
				continue
			}
			idx[e.Key] = len(groups)
		}
		groups = append(groups, []Event{e})
	}
	return groups
}

func retry(ctx context.Context, fn func() error) error {
	var err error
	for i := range 3 {
		if err = fn(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(i+1) * 100 * time.Millisecond):
		}
	}
	return err
}
