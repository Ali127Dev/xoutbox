package xoutbox

import (
	"context"
	"errors"
)

// Publisher delivers a single event to a broker. Publish must return only after
// the broker has durably accepted the message.
type Publisher interface {
	Publish(ctx context.Context, e Event) error
}

// BatchPublisher is an optional fast path. The returned slice must have one
// entry per event (nil on success). The worker uses it automatically.
type BatchPublisher interface {
	Publisher
	PublishBatch(ctx context.Context, events []Event) []error
}

// PublisherFunc adapts a function to the Publisher interface.
type PublisherFunc func(ctx context.Context, e Event) error

func (f PublisherFunc) Publish(ctx context.Context, e Event) error { return f(ctx, e) }

type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent marks err as non-retryable: the event goes straight to the dead-letter state.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// ErrNoTopic is returned when neither the event nor the publisher defines a destination.
var ErrNoTopic = Permanent(errors.New("xoutbox: event has no topic"))

// ResolveTopic returns e.Topic, falling back to def.
func ResolveTopic(e Event, def string) (string, error) {
	if e.Topic != "" {
		return e.Topic, nil
	}
	if def != "" {
		return def, nil
	}
	return "", ErrNoTopic
}

// Meta returns the metadata headers merged with the event headers.
func Meta(e Event) map[string]string {
	h := make(map[string]string, len(e.Headers)+3)
	for k, v := range e.Headers {
		h[k] = v
	}
	h[HeaderID] = e.ID
	if e.Type != "" {
		h[HeaderType] = e.Type
	}
	if e.Key != "" {
		h[HeaderKey] = e.Key
	}
	return h
}
