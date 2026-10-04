# xoutbox

[![Go Reference](https://pkg.go.dev/badge/github.com/Ali127Dev/xoutbox.svg)](https://pkg.go.dev/github.com/Ali127Dev/xoutbox)
[![CI](https://github.com/Ali127Dev/xoutbox/actions/workflows/ci.yml/badge.svg)](https://github.com/Ali127Dev/xoutbox/actions/workflows/ci.yml)

**A production-grade [Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html) for Go.**
Write events in the same database transaction as your business data, and xoutbox delivers them to
**Kafka, NATS JetStream, RabbitMQ or Redis** with no lost messages, crash recovery and
per-key ordering. You don't need distributed transactions.

```
┌──────────── your service ────────────┐          ┌──────────────┐
│ BEGIN                                │          │    Kafka     │
│   INSERT INTO orders ...             │  Worker  │    NATS      │
│   INSERT INTO outbox ...  ◄──────────┼──claim──►│  RabbitMQ    │
│ COMMIT                               │  publish │    Redis     │
└──────────────────────────────────────┘          └──────────────┘
```

## Features

| | |
|---|---|
| **Atomic writes** | `store.Insert(ctx, tx, events...)` accepts any `*sql.Tx` / `*sql.DB` / `*sql.Conn` |
| **Horizontal scaling** | Run any number of workers on any number of pods. They claim rows with `FOR UPDATE SKIP LOCKED` and **leases** |
| **Crash recovery** | If a worker dies mid-batch, its lease expires and another worker picks the events up |
| **Per-key ordering** | Events with the same `Key` are published in order. `WithOrdering()` gives strict head-of-line ordering across workers |
| **Retries with backoff** | Exponential backoff with jitter, a per-event `MaxAttempts`, and `xoutbox.Permanent(err)` to skip retries |
| **Dead-letter queue** | Exhausted events move to `dead`. `Requeue()` replays them |
| **Poison-pill protection** | Attempts are counted on *claim*, so an event that crashes the worker can't loop forever |
| **Delayed / scheduled events** | `event.WithDelay(10*time.Minute)` |
| **Batch publishing** | Kafka `SendMessages`, NATS async acks, RabbitMQ pipelined confirms, Redis pipelines |
| **Low latency** | Adaptive polling (re-polls immediately when a batch is full) plus `worker.Notify()` after commit |
| **Graceful shutdown** | The in-flight batch is always finished and committed before `Run` returns |
| **Observability** | `log/slog` logging, `Hooks` for metrics/tracing, `Stats()` for backlog gauges |
| **Housekeeping** | Automatic retention cleanup of published rows in batches, plus a reaper for stuck rows |
| **Panic safe** | A panicking publisher counts as a failed attempt and doesn't crash the worker |
| **Tested** | Unit tests plus integration tests against real Postgres, MySQL, Kafka, NATS, RabbitMQ and Redis |

## Install

```bash
go get github.com/Ali127Dev/xoutbox
```

| Package | Backend | Notes |
|---|---|---|
| `xoutbox/postgres` | PostgreSQL 12+ | Single-round-trip claim (`UPDATE … RETURNING`). Works with `pgx/stdlib` and `lib/pq` |
| `xoutbox/mysql` | MySQL 8.0+, MariaDB 10.6+ | `go-sql-driver/mysql` |
| `xoutbox/kafka` | Apache Kafka | IBM/sarama, idempotent producer, `acks=all`, key-hash partitioning |
| `xoutbox/nats` | NATS JetStream | `Nats-Msg-Id` = event ID, so it's **deduplicated** inside the stream's duplicate window |
| `xoutbox/rabbitmq` | RabbitMQ | Publisher confirms, persistent messages, auto-reconnect |
| `xoutbox/redis` | Redis Streams / Pub/Sub | Standalone, Sentinel and Cluster (`UniversalClient`), `MAXLEN ~` trimming |

## Quick start (Postgres → Kafka)

```go
db, _ := sql.Open("pgx", os.Getenv("DATABASE_URL"))

store := postgres.New(db)                // postgres.WithTable("events_outbox"), postgres.WithOrdering(), ...
if err := store.Migrate(ctx); err != nil { // or copy postgres/schema.sql into your migrations
    log.Fatal(err)
}

pub, err := kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}})
if err != nil {
    log.Fatal(err)
}
defer pub.Close()

worker := xoutbox.NewWorker(store, pub,
    xoutbox.WithBatchSize(200),
    xoutbox.WithConcurrency(16),
    xoutbox.WithCleanup(time.Minute, 72*time.Hour), // delete published rows after 3 days
)
go worker.Run(ctx) // returns nil once ctx is cancelled and the in-flight batch is committed
```

### Writing events in your transaction

```go
func (s *Service) CreateOrder(ctx context.Context, o Order) error {
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    if _, err := tx.ExecContext(ctx, `INSERT INTO orders (id, total) VALUES ($1, $2)`, o.ID, o.Total); err != nil {
        return err
    }

    evt, err := xoutbox.NewJSONEvent("orders", "order.created", o) // topic, type, payload
    if err != nil {
        return err
    }
    evt = evt.WithKey(o.CustomerID).WithHeader("trace-id", traceID(ctx))

    if err := s.outbox.Insert(ctx, tx, evt); err != nil {
        return err
    }
    if err := tx.Commit(); err != nil {
        return err
    }
    s.worker.Notify() // optional: publish right away instead of waiting for the next poll
    return nil
}
```

## Other brokers

```go
// NATS JetStream (exactly-once into the stream thanks to Nats-Msg-Id dedup)
nc, _ := nats.Connect(nats.DefaultURL)
js, _ := jetstream.New(nc)
pub := xnats.New(js, xnats.WithSubjectPrefix("events."))     // topic "orders" → subject "events.orders"

// RabbitMQ (confirms + persistent + auto-reconnect)
pub, _ := rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Exchange: "events"})

// Redis Streams
rdb := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: []string{"localhost:6379"}})
pub := xredis.New(rdb, xredis.Config{MaxLen: 1_000_000})     // xredis.PubSub for fire-and-forget

// Anything else
pub := xoutbox.PublisherFunc(func(ctx context.Context, e xoutbox.Event) error {
    return myClient.Send(ctx, e.Topic, e.Payload)
})
```

How `Event` fields map to each broker:

| Event | Kafka | NATS | RabbitMQ | Redis Stream |
|---|---|---|---|---|
| `Topic` | topic | subject (+prefix) | routing key (queue name on default exchange) | stream key (+prefix) |
| `Key` | message key (partition) | `outbox-key` header | `outbox-key` header | `key` field |
| `ID` | `outbox-id` header | `Nats-Msg-Id` + header | `MessageId` + header | `id` field |
| `Type` | `outbox-type` header | header | `Type` + header | `type` field |
| `Headers` | record headers | headers | headers | `headers` field (JSON) |

To route to different brokers, write a `PublisherFunc` that switches on `e.Topic` or `e.Type`.

## Delivery guarantees

xoutbox gives you **at-least-once** delivery. A message can be published twice if the broker accepts
it but the worker crashes before marking it published. Make consumers idempotent by using the
`outbox-id` header / message ID as a dedup key. NATS JetStream does this for you.

**Ordering.** Inside one batch, events that share a `Key` are published one after another. If one
fails, the rest of its group is put back without using up an attempt. For strict ordering across
retries and multiple workers, enable `postgres.WithOrdering()` / `mysql.WithOrdering()`: an event is
only claimed once every older event with the same key has been published or dead-lettered. Events
with an empty key are never blocked.

## Worker options

| Option | Default | Description |
|---|---|---|
| `WithBatchSize(n)` | 100 | Events claimed per poll |
| `WithConcurrency(n)` | 8 | Parallel publishes (per key group) when the publisher isn't a `BatchPublisher` |
| `WithPollInterval(d)` | 1s | Idle poll interval. A full batch re-polls immediately |
| `WithLease(d)` | max(30s, 3×timeout) | How long claimed rows stay locked to this worker |
| `WithPublishTimeout(d)` | 10s | Timeout per `Publish` / `PublishBatch` |
| `WithBackoff(b)` | exp 1s→5m + jitter | Retry delay policy: `ExponentialBackoff(base, max)` or your own func |
| `WithCleanup(every, retention)` | 1m, keep forever | Housekeeping interval and how long published rows are kept |
| `WithWorkerID(id)` | host-pid-rand | Lease owner name |
| `WithLogger(l)` | `slog.Default()` | Structured logger |
| `WithHooks(h)` | none | `OnPublished`, `OnFailed(e, err, dead)`, `OnStoreError(op, err)` |

`worker.RunOnce(ctx)` processes a single batch, which is handy for cron jobs, Lambdas and tests.

## Metrics (Prometheus example)

```go
published := promauto.NewCounterVec(prometheus.CounterOpts{Name: "outbox_published_total"}, []string{"topic"})
failed    := promauto.NewCounterVec(prometheus.CounterOpts{Name: "outbox_failed_total"}, []string{"topic", "dead"})
lag       := promauto.NewHistogram(prometheus.HistogramOpts{Name: "outbox_lag_seconds"})

worker := xoutbox.NewWorker(store, pub, xoutbox.WithHooks(xoutbox.Hooks{
    OnPublished: func(e xoutbox.Event) {
        published.WithLabelValues(e.Topic).Inc()
        lag.Observe(time.Since(e.CreatedAt).Seconds())
    },
    OnFailed: func(e xoutbox.Event, err error, dead bool) {
        failed.WithLabelValues(e.Topic, strconv.FormatBool(dead)).Inc()
    },
}))

// Backlog gauges: scrape store.Stats(ctx) → Pending, Processing, Dead, OldestPending
```

## Operations

```go
n, err := store.Requeue(ctx)            // replay every dead event
n, err := store.Requeue(ctx, id1, id2)  // replay specific events
st, err := store.Stats(ctx)             // backlog snapshot
deleted, err := store.Cleanup(ctx, 24*time.Hour)
fmt.Println(store.SchemaSQL())          // DDL for your migration tool
```

```sql
-- inspect the dead-letter queue
SELECT id, topic, event_type, attempts, last_error, created_at FROM outbox WHERE status = 'dead';
```

## Schema

`store.Migrate(ctx)` creates the table and indexes idempotently. You can also copy
[`postgres/schema.sql`](postgres/schema.sql) or [`mysql/schema.sql`](mysql/schema.sql) into your
migrations (replace `{t}`/`{n}` with the table name). Key columns: `seq` (insertion order), `id`, `topic`,
`partition_key`, `payload`, `headers`, `status`, `attempts`, `max_attempts`, `locked_by`,
`locked_until`, `available_at`, `last_error`.

All timestamps come from the **database clock**, so app-server clock skew and timezone settings
don't matter.

## Custom backends

Implement `xoutbox.Store` for another database, or `xoutbox.Publisher` / `xoutbox.BatchPublisher`
for another broker. `internal/storetest` holds the conformance suite used by the SQL stores.

```go
type Store interface {
    Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]Event, error)
    MarkPublished(ctx context.Context, ids []string) error
    MarkFailed(ctx context.Context, owner string, f Failure) error
}
```

## Development

```bash
make test               # unit tests
make test-integration   # starts Postgres, MySQL, Kafka, NATS, RabbitMQ, Redis via docker compose
```

## License

MIT
