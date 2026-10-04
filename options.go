package xoutbox

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"time"
)

// Backoff returns the delay before the next attempt, given the attempts made so far.
type Backoff func(attempt int) time.Duration

// ExponentialBackoff doubles the delay per attempt up to max, with equal jitter.
func ExponentialBackoff(base, max time.Duration) Backoff {
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		d := base << min(attempt-1, 32)
		if d <= 0 || d > max {
			d = max
		}
		half := d / 2
		return half + time.Duration(rand.Int64N(int64(half)+1))
	}
}

// Hooks are optional callbacks for metrics and tracing. They must not block.
type Hooks struct {
	OnPublished  func(e Event)
	OnFailed     func(e Event, err error, dead bool)
	OnStoreError func(op string, err error)
}

type options struct {
	batchSize       int
	concurrency     int
	pollInterval    time.Duration
	lease           time.Duration
	publishTimeout  time.Duration
	backoff         Backoff
	cleanupInterval time.Duration
	retention       time.Duration
	owner           string
	logger          *slog.Logger
	hooks           Hooks
}

// Option configures a Worker.
type Option func(*options)

// WithBatchSize sets how many events are claimed per poll (default 100).
func WithBatchSize(n int) Option { return func(o *options) { o.batchSize = n } }

// WithConcurrency sets how many events/key-groups are published in parallel (default 8).
func WithConcurrency(n int) Option { return func(o *options) { o.concurrency = n } }

// WithPollInterval sets the idle polling interval (default 1s). When a batch is
// full the worker polls again immediately.
func WithPollInterval(d time.Duration) Option { return func(o *options) { o.pollInterval = d } }

// WithLease sets how long claimed events stay locked to this worker (default 30s).
// It must comfortably exceed the time needed to publish a batch.
func WithLease(d time.Duration) Option { return func(o *options) { o.lease = d } }

// WithPublishTimeout bounds each Publish/PublishBatch call (default 10s).
func WithPublishTimeout(d time.Duration) Option { return func(o *options) { o.publishTimeout = d } }

// WithBackoff sets the retry delay policy (default exponential 1s..5m with jitter).
func WithBackoff(b Backoff) Option { return func(o *options) { o.backoff = b } }

// WithCleanup sets how often housekeeping runs (default 1m, 0 disables) and how long
// published events are retained (default 0 = keep forever).
func WithCleanup(interval, retention time.Duration) Option {
	return func(o *options) { o.cleanupInterval, o.retention = interval, retention }
}

// WithWorkerID sets the lease owner name (default host-pid-random).
func WithWorkerID(id string) Option { return func(o *options) { o.owner = id } }

// WithLogger sets the structured logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

// WithHooks installs metrics/tracing callbacks.
func WithHooks(h Hooks) Option { return func(o *options) { o.hooks = h } }

func newOptions(opts []Option) options {
	o := options{cleanupInterval: time.Minute}
	for _, opt := range opts {
		opt(&o)
	}
	if o.batchSize <= 0 {
		o.batchSize = 100
	}
	if o.concurrency <= 0 {
		o.concurrency = 8
	}
	if o.pollInterval <= 0 {
		o.pollInterval = time.Second
	}
	if o.publishTimeout <= 0 {
		o.publishTimeout = 10 * time.Second
	}
	if o.lease <= 0 {
		o.lease = max(30*time.Second, 3*o.publishTimeout)
	}
	if o.backoff == nil {
		o.backoff = ExponentialBackoff(time.Second, 5*time.Minute)
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	if o.owner == "" {
		host, _ := os.Hostname()
		o.owner = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), NewID()[24:])
	}
	return o
}
