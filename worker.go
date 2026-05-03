package xoutbox

import (
	"context"
	"sync"
	"time"
)

type WorkerConfig struct {
	Interval    time.Duration
	BatchSize   int
	Concurrency int
}

func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		Interval:    1 * time.Second,
		BatchSize:   50,
		Concurrency: 4,
	}
}

type Worker[T comparable] struct {
	store     Store[T]
	publisher Publisher[T]
	cfg       WorkerConfig
}

func NewWorker[T comparable](store Store[T], publisher Publisher[T], cfg WorkerConfig) *Worker[T] {
	return &Worker[T]{
		store:     store,
		publisher: publisher,
		cfg:       cfg,
	}
}

func (w *Worker[T]) Start(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if err := w.processBatch(ctx); err != nil {
				continue
			}
		}
	}
}

func (w *Worker[T]) processBatch(ctx context.Context) error {
	events, err := w.store.FetchPending(ctx, w.cfg.BatchSize)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		return nil
	}

	sem := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup

	for _, evt := range events {
		wg.Add(1)

		sem <- struct{}{}

		go func(e Event[T]) {
			defer wg.Done()
			defer func() { <-sem }()

			_ = w.processEvent(ctx, e)
		}(evt)
	}

	wg.Wait()
	return nil
}

func (w *Worker[T]) processEvent(ctx context.Context, e Event[T]) error {
	err := w.publisher.Publish(ctx, e)
	if err != nil {
		newRetry := e.RetryCount + 1
		return w.store.MarkFailed(ctx, e.ID, newRetry)
	}

	return w.store.MarkPublished(ctx, e.ID)
}
