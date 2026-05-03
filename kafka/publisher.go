package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Ali127Dev/xoutbox"
	"github.com/IBM/sarama"
)

type Publisher[T comparable] struct {
	producer sarama.AsyncProducer
	topic    string
}

type Config struct {
	Brokers      []string
	Topic        string
	RequiredAcks sarama.RequiredAcks
	BatchSize    int
	BatchTimeout time.Duration
}

func NewPublisher[T comparable](cfg Config) (*Publisher[T], error) {
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = cfg.RequiredAcks

	config.Producer.Flush.Messages = cfg.BatchSize
	config.Producer.Flush.Frequency = cfg.BatchTimeout

	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = false

	producer, err := sarama.NewAsyncProducer(cfg.Brokers, config)
	if err != nil {
		return nil, err
	}

	p := &Publisher[T]{
		producer: producer,
		topic:    cfg.Topic,
	}

	return p, nil
}

func (p *Publisher[T]) Publish(ctx context.Context, event xoutbox.Event[T]) error {
	key := fmt.Sprintf("%v", event.ID)

	msg := &sarama.ProducerMessage{
		Topic:     p.topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(event.Payload),
		Timestamp: time.Now(),
	}

	select {
	case p.producer.Input() <- msg:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Publisher[T]) Close() error {
	return p.producer.Close()
}
