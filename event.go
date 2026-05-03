package xoutbox

import "time"

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusPublished  Status = "published"
	StatusFailed     Status = "failed"
	StatusDead       Status = "dead"
)

type Event[T comparable] struct {
	ID        T
	EventType string
	Payload   []byte

	RetryCount int
	MaxRetries int

	Status Status

	CreatedAt   time.Time
	PublishedAt *time.Time
}
