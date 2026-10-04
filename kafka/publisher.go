// Package kafka publishes outbox events to Apache Kafka using IBM/sarama.
package kafka

import (
	"context"
	"errors"

	"github.com/Ali127Dev/xoutbox"
	"github.com/IBM/sarama"
)

// Config configures the publisher.
type Config struct {
	Brokers []string
	// DefaultTopic is used when Event.Topic is empty.
	DefaultTopic string
	// Sarama overrides the producer config. Nil uses NewSaramaConfig().
	// Producer.Return.Successes is always forced to true.
	Sarama *sarama.Config
}

// NewSaramaConfig returns a durable producer config: acks=all, idempotent,
// ordered retries and hash partitioning by Event.Key.
func NewSaramaConfig() *sarama.Config {
	c := sarama.NewConfig()
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Idempotent = true
	c.Net.MaxOpenRequests = 1
	c.Producer.Retry.Max = 10
	c.Producer.Return.Successes = true
	c.Producer.Return.Errors = true
	c.Producer.Partitioner = sarama.NewHashPartitioner
	return c
}

// Publisher implements xoutbox.BatchPublisher.
type Publisher struct {
	producer sarama.SyncProducer
	topic    string
}

var _ xoutbox.BatchPublisher = (*Publisher)(nil)

// New creates a publisher with its own producer. Call Close when done.
func New(cfg Config) (*Publisher, error) {
	sc := cfg.Sarama
	if sc == nil {
		sc = NewSaramaConfig()
	}
	sc.Producer.Return.Successes = true
	sc.Producer.Return.Errors = true
	p, err := sarama.NewSyncProducer(cfg.Brokers, sc)
	if err != nil {
		return nil, err
	}
	return &Publisher{producer: p, topic: cfg.DefaultTopic}, nil
}

// NewFromProducer wraps an existing SyncProducer.
func NewFromProducer(p sarama.SyncProducer, defaultTopic string) *Publisher {
	return &Publisher{producer: p, topic: defaultTopic}
}

func (p *Publisher) message(e xoutbox.Event) (*sarama.ProducerMessage, error) {
	topic, err := xoutbox.ResolveTopic(e, p.topic)
	if err != nil {
		return nil, err
	}
	meta := xoutbox.Meta(e)
	headers := make([]sarama.RecordHeader, 0, len(meta))
	for k, v := range meta {
		headers = append(headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}
	m := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(e.Payload), Headers: headers, Timestamp: e.CreatedAt}
	if e.Key != "" {
		m.Key = sarama.StringEncoder(e.Key)
	}
	return m, nil
}

func (p *Publisher) Publish(ctx context.Context, e xoutbox.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m, err := p.message(e)
	if err != nil {
		return err
	}
	_, _, err = p.producer.SendMessage(m)
	return classify(err)
}

func (p *Publisher) PublishBatch(ctx context.Context, events []xoutbox.Event) []error {
	errs := make([]error, len(events))
	if err := ctx.Err(); err != nil {
		for i := range errs {
			errs[i] = err
		}
		return errs
	}
	msgs := make([]*sarama.ProducerMessage, 0, len(events))
	idx := make(map[*sarama.ProducerMessage]int, len(events))
	for i, e := range events {
		m, err := p.message(e)
		if err != nil {
			errs[i] = err
			continue
		}
		msgs = append(msgs, m)
		idx[m] = i
	}
	if err := p.producer.SendMessages(msgs); err != nil {
		var pe sarama.ProducerErrors
		if errors.As(err, &pe) {
			for _, x := range pe {
				if i, ok := idx[x.Msg]; ok {
					errs[i] = classify(x.Err)
				}
			}
		} else {
			for _, i := range idx {
				errs[i] = err
			}
		}
	}
	return errs
}

func classify(err error) error {
	if errors.Is(err, sarama.ErrMessageSizeTooLarge) || errors.Is(err, sarama.ErrInvalidMessage) {
		return xoutbox.Permanent(err)
	}
	return err
}

// Close closes the underlying producer.
func (p *Publisher) Close() error { return p.producer.Close() }
