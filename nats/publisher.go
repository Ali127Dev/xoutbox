// Package nats publishes outbox events to NATS JetStream. The event ID is sent as
// Nats-Msg-Id, so JetStream de-duplicates redeliveries inside the stream's
// duplicate window — effectively exactly-once into the stream.
package nats

import (
	"context"

	"github.com/Ali127Dev/xoutbox"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Publisher implements xoutbox.BatchPublisher.
type Publisher struct {
	js       jetstream.JetStream
	prefix   string
	fallback string
}

var _ xoutbox.BatchPublisher = (*Publisher)(nil)

// Option configures the publisher.
type Option func(*Publisher)

// WithSubjectPrefix prepends prefix to every subject, e.g. "events." .
func WithSubjectPrefix(prefix string) Option { return func(p *Publisher) { p.prefix = prefix } }

// WithDefaultSubject is used when Event.Topic is empty.
func WithDefaultSubject(s string) Option { return func(p *Publisher) { p.fallback = s } }

func New(js jetstream.JetStream, opts ...Option) *Publisher {
	p := &Publisher{js: js}
	for _, o := range opts {
		o(p)
	}
	return p
}

func (p *Publisher) msg(e xoutbox.Event) (*natsgo.Msg, error) {
	subject, err := xoutbox.ResolveTopic(e, p.fallback)
	if err != nil {
		return nil, err
	}
	m := natsgo.NewMsg(p.prefix + subject)
	m.Data = e.Payload
	for k, v := range xoutbox.Meta(e) {
		m.Header.Set(k, v)
	}
	return m, nil
}

func (p *Publisher) Publish(ctx context.Context, e xoutbox.Event) error {
	m, err := p.msg(e)
	if err != nil {
		return err
	}
	_, err = p.js.PublishMsg(ctx, m, jetstream.WithMsgID(e.ID))
	return err
}

// PublishBatch pipelines all messages with async publishing and waits for every ack.
func (p *Publisher) PublishBatch(ctx context.Context, events []xoutbox.Event) []error {
	errs := make([]error, len(events))
	futures := make([]jetstream.PubAckFuture, len(events))
	for i, e := range events {
		m, err := p.msg(e)
		if err == nil {
			futures[i], err = p.js.PublishMsgAsync(m, jetstream.WithMsgID(e.ID))
		}
		errs[i] = err
	}
	for i, f := range futures {
		if f == nil {
			continue
		}
		select {
		case <-f.Ok():
		case err := <-f.Err():
			errs[i] = err
		case <-ctx.Done():
			errs[i] = ctx.Err()
		}
	}
	return errs
}
