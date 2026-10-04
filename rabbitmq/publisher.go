// Package rabbitmq publishes outbox events to RabbitMQ with publisher confirms,
// persistent delivery and automatic reconnection.
//
// Unroutable messages are confirmed by the broker and silently dropped; configure
// an alternate-exchange on your exchange if that matters for you.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Ali127Dev/xoutbox"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Config configures the publisher.
type Config struct {
	URL string
	// Exchange to publish to. Empty means the default exchange, where Event.Topic
	// is the destination queue name. Otherwise Event.Topic is the routing key.
	Exchange string
	// DefaultRoutingKey is used when Event.Topic is empty.
	DefaultRoutingKey string
	// AMQP optionally customises the connection (TLS, heartbeat, ...).
	AMQP *amqp.Config
}

// Publisher implements xoutbox.BatchPublisher. Safe for concurrent use.
type Publisher struct {
	cfg  Config
	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

var _ xoutbox.BatchPublisher = (*Publisher)(nil)

// ErrNack is returned when the broker negatively acknowledges a message.
var ErrNack = errors.New("rabbitmq: message nacked by broker")

// New dials the broker and opens a confirm-mode channel.
func New(cfg Config) (*Publisher, error) {
	p := &Publisher{cfg: cfg}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := p.channel(); err != nil {
		return nil, err
	}
	return p, nil
}

// channel returns a live confirm channel, reconnecting if needed. mu must be held.
func (p *Publisher) channel() (*amqp.Channel, error) {
	if p.ch != nil && !p.ch.IsClosed() {
		return p.ch, nil
	}
	if p.conn == nil || p.conn.IsClosed() {
		var err error
		if p.cfg.AMQP != nil {
			p.conn, err = amqp.DialConfig(p.cfg.URL, *p.cfg.AMQP)
		} else {
			p.conn, err = amqp.Dial(p.cfg.URL)
		}
		if err != nil {
			return nil, fmt.Errorf("rabbitmq: dial: %w", err)
		}
	}
	ch, err := p.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("rabbitmq: confirm mode: %w", err)
	}
	p.ch = ch
	return ch, nil
}

func (p *Publisher) send(ctx context.Context, ch *amqp.Channel, e xoutbox.Event) (*amqp.DeferredConfirmation, error) {
	rk, err := xoutbox.ResolveTopic(e, p.cfg.DefaultRoutingKey)
	if err != nil {
		return nil, err
	}
	meta := xoutbox.Meta(e)
	headers := make(amqp.Table, len(meta))
	for k, v := range meta {
		headers[k] = v
	}
	return ch.PublishWithDeferredConfirmWithContext(ctx, p.cfg.Exchange, rk, false, false, amqp.Publishing{
		MessageId:    e.ID,
		Type:         e.Type,
		ContentType:  e.Headers["content-type"],
		DeliveryMode: amqp.Persistent,
		Timestamp:    e.CreatedAt,
		Headers:      headers,
		Body:         e.Payload,
	})
}

func wait(ctx context.Context, dc *amqp.DeferredConfirmation) error {
	ok, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNack
	}
	return nil
}

func (p *Publisher) Publish(ctx context.Context, e xoutbox.Event) error {
	return p.PublishBatch(ctx, []xoutbox.Event{e})[0]
}

// PublishBatch publishes all messages, then waits for every confirm.
func (p *Publisher) PublishBatch(ctx context.Context, events []xoutbox.Event) []error {
	errs := make([]error, len(events))
	dcs := make([]*amqp.DeferredConfirmation, len(events))

	p.mu.Lock()
	ch, err := p.channel()
	if err == nil {
		for i, e := range events {
			dcs[i], errs[i] = p.send(ctx, ch, e)
		}
	}
	p.mu.Unlock()

	for i := range events {
		switch {
		case err != nil:
			errs[i] = err
		case errs[i] == nil && dcs[i] != nil:
			errs[i] = wait(ctx, dcs[i])
		}
	}
	return errs
}

// Close closes the channel and connection.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch != nil {
		_ = p.ch.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
