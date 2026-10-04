package xoutbox

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"maps"
	"time"
)

// Status is the lifecycle state of an outbox row.
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusPublished  Status = "published"
	StatusDead       Status = "dead"
)

// Metadata headers attached to every message by the built-in publishers.
const (
	HeaderID   = "outbox-id"
	HeaderType = "outbox-type"
	HeaderKey  = "outbox-key"
)

// Event is a message stored in the outbox and relayed to a broker.
type Event struct {
	// ID uniquely identifies the event. NewEvent fills it with a UUIDv7.
	// Brokers that support de-duplication (NATS JetStream) use it as the message ID.
	ID string
	// Topic is the destination: Kafka topic, NATS subject, RabbitMQ routing key or Redis stream.
	Topic string
	// Key is the partition / ordering key (e.g. an aggregate ID). Events sharing a key
	// are published in insertion order. Empty means unordered.
	Key string
	// Type is a free-form event name such as "order.created".
	Type    string
	Payload []byte
	Headers map[string]string

	// AvailableAt delays delivery until the given time. Zero means immediately.
	AvailableAt time.Time
	// MaxAttempts overrides the store default. Zero means the store default.
	MaxAttempts int

	// Set by the store when an event is claimed.
	Attempts  int
	CreatedAt time.Time
}

// NewEvent creates an event with a fresh time-ordered ID.
func NewEvent(topic, eventType string, payload []byte) Event {
	return Event{ID: NewID(), Topic: topic, Type: eventType, Payload: payload}
}

// NewJSONEvent marshals v as the payload and sets content-type to application/json.
func NewJSONEvent(topic, eventType string, v any) (Event, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return Event{}, err
	}
	return NewEvent(topic, eventType, b).WithHeader("content-type", "application/json"), nil
}

// WithKey returns a copy of e with the ordering key set.
func (e Event) WithKey(key string) Event { e.Key = key; return e }

// WithHeader returns a copy of e with the header set.
func (e Event) WithHeader(k, v string) Event {
	h := make(map[string]string, len(e.Headers)+1)
	maps.Copy(h, e.Headers)
	h[k] = v
	e.Headers = h
	return e
}

// WithDelay returns a copy of e that will not be published before d has elapsed.
func (e Event) WithDelay(d time.Duration) Event { e.AvailableAt = time.Now().Add(d); return e }

// WithMaxAttempts returns a copy of e with a custom attempt limit.
func (e Event) WithMaxAttempts(n int) Event { e.MaxAttempts = n; return e }

// NewID returns a random UUIDv7 (time-ordered, index friendly).
func NewID() string {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	for i := 0; i < 6; i++ {
		b[i] = byte(ms >> (40 - 8*i))
	}
	_, _ = rand.Read(b[6:])
	b[6] = b[6]&0x0f | 0x70
	b[8] = b[8]&0x3f | 0x80

	var s [36]byte
	hex.Encode(s[0:8], b[0:4])
	s[8] = '-'
	hex.Encode(s[9:13], b[4:6])
	s[13] = '-'
	hex.Encode(s[14:18], b[6:8])
	s[18] = '-'
	hex.Encode(s[19:23], b[8:10])
	s[23] = '-'
	hex.Encode(s[24:], b[10:])
	return string(s[:])
}
