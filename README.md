
# xoutbox

🚀 **A lightweight, production‑ready implementation of the Transactional Outbox Pattern for Go.**

`xoutbox` helps you reliably publish events to message brokers (Kafka, NATS, RabbitMQ, etc.) **without losing messages** and **without distributed transactions**.

It ensures **atomicity between database writes and event publishing** using the **Outbox Pattern**.

Repository:  
https://github.com/Ali127Dev/xoutbox

---

# ✨ Features

✅ Transactional Outbox pattern  
✅ Broker‑agnostic (Kafka, NATS, RabbitMQ, etc.)  
✅ Pluggable storage (Postgres, MySQL, etc.)  
✅ Generic event ID support  
✅ Safe concurrent workers  
✅ Retry & dead‑letter support  
✅ Simple interfaces  
✅ Production‑friendly

---

# 🧠 The Problem

Imagine a typical service:

```go
db.Save(order)
broker.Publish(OrderCreated)
```

If the service crashes between these two operations:

- ✅ Order saved
- ❌ Event never published

Your system becomes **inconsistent**.

Distributed transactions are heavy and usually avoided.

---

# ✅ The Solution: Transactional Outbox Pattern

Instead of publishing events directly, we store them in an **outbox table** inside the same database transaction.

```
Service
   │
   ├── Save business data
   ├── Insert event into outbox
   │
   ▼
Database
   │
   ▼
Outbox Worker
   │
   ▼
Message Broker
```

Result:

✅ No lost messages  
✅ At‑least‑once delivery  
✅ Reliable event publishing

---

# 📦 Installation

```bash
go get github.com/Ali127Dev/xoutbox
```

---

# 📐 Core Concepts

`xoutbox` is built around two main interfaces:

- **Publisher**
- **Store**

These allow the library to remain **broker‑agnostic** and **database‑agnostic**.

---

# 🧾 Event Model

```go
type Event[T comparable] struct {
    ID          T
    EventType   string
    Payload     []byte
    Status      Status
    RetryCount  int
    MaxRetries  int
    CreatedAt   time.Time
    PublishedAt *time.Time
}
```

---

# 📡 Publisher Interface

```go
type Publisher[T comparable] interface {
    Publish(ctx context.Context, event Event[T]) error
}
```

---

# 🗄 Database Schema Example

```sql
CREATE TABLE outbox (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 5,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP
);
```

---

# 🔌 Kafka Publisher (Sarama Example)

```go
type KafkaPublisher struct {
    producer sarama.AsyncProducer
    topic    string
}

func (p *KafkaPublisher) Publish(ctx context.Context, event xoutbox.Event[string]) error {
    msg := &sarama.ProducerMessage{
        Topic: p.topic,
        Key:   sarama.StringEncoder(event.ID),
        Value: sarama.ByteEncoder(event.Payload),
    }

    select {
    case p.producer.Input() <- msg:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

---

# ⚙️ Worker Overview

1️⃣ Fetch events  
2️⃣ Publish them  
3️⃣ Mark processed or failed  
4️⃣ Supports concurrency safely

---

# 📊 Delivery Semantics

xoutbox guarantees:

**At‑least‑once delivery**

Consumers must therefore be idempotent.

---

# ❤️ Author

**Ali127Dev**  
https://github.com/Ali127Dev

