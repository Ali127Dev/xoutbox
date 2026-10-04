// Package redis publishes outbox events to Redis Streams (durable, default) or
// Redis Pub/Sub (fire-and-forget). Works with standalone, Sentinel and Cluster.
package redis

import (
	"context"
	"encoding/json"

	"github.com/Ali127Dev/xoutbox"
	goredis "github.com/redis/go-redis/v9"
)

// Mode selects the Redis primitive.
type Mode int

const (
	// Stream appends to a stream with XADD (recommended; consumer groups get at-least-once).
	Stream Mode = iota
	// PubSub uses PUBLISH. Subscribers that are offline miss messages.
	PubSub
)

// Config configures the publisher.
type Config struct {
	Mode Mode
	// DefaultStream is used when Event.Topic is empty.
	DefaultStream string
	// Prefix is prepended to every stream/channel name.
	Prefix string
	// MaxLen caps streams approximately (XADD MAXLEN ~). Zero means unbounded.
	MaxLen int64
}

// Publisher implements xoutbox.BatchPublisher.
type Publisher struct {
	c   goredis.UniversalClient
	cfg Config
}

var _ xoutbox.BatchPublisher = (*Publisher)(nil)

func New(c goredis.UniversalClient, cfg Config) *Publisher { return &Publisher{c: c, cfg: cfg} }

func (p *Publisher) cmd(ctx context.Context, c goredis.Cmdable, e xoutbox.Event) (goredis.Cmder, error) {
	topic, err := xoutbox.ResolveTopic(e, p.cfg.DefaultStream)
	if err != nil {
		return nil, err
	}
	topic = p.cfg.Prefix + topic
	if p.cfg.Mode == PubSub {
		return c.Publish(ctx, topic, e.Payload), nil
	}
	values := []any{"id", e.ID, "type", e.Type, "key", e.Key, "payload", e.Payload}
	if len(e.Headers) > 0 {
		h, err := json.Marshal(e.Headers)
		if err != nil {
			return nil, xoutbox.Permanent(err)
		}
		values = append(values, "headers", h)
	}
	return c.XAdd(ctx, &goredis.XAddArgs{Stream: topic, MaxLen: p.cfg.MaxLen, Approx: p.cfg.MaxLen > 0, Values: values}), nil
}

func (p *Publisher) Publish(ctx context.Context, e xoutbox.Event) error {
	c, err := p.cmd(ctx, p.c, e)
	if err != nil {
		return err
	}
	return c.Err()
}

// PublishBatch sends all commands in a single pipeline round trip.
func (p *Publisher) PublishBatch(ctx context.Context, events []xoutbox.Event) []error {
	errs := make([]error, len(events))
	cmds := make([]goredis.Cmder, len(events))
	pipe := p.c.Pipeline()
	for i, e := range events {
		cmds[i], errs[i] = p.cmd(ctx, pipe, e)
	}
	_, _ = pipe.Exec(ctx)
	for i, c := range cmds {
		if c != nil {
			errs[i] = c.Err()
		}
	}
	return errs
}
