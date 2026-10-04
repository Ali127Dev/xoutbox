package kafka_test

import (
	"context"
	"os"
	"testing"

	"github.com/Ali127Dev/xoutbox"
	"github.com/Ali127Dev/xoutbox/kafka"
)

// XOUTBOX_KAFKA_BROKERS=localhost:9092
func TestPublisher(t *testing.T) {
	brokers := os.Getenv("XOUTBOX_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("XOUTBOX_KAFKA_BROKERS not set")
	}
	p, err := kafka.New(kafka.Config{Brokers: []string{brokers}, DefaultTopic: "xoutbox-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()
	if err := p.Publish(ctx, xoutbox.NewEvent("", "x", []byte("one")).WithKey("k")); err != nil {
		t.Fatal(err)
	}
	for _, err := range p.PublishBatch(ctx, []xoutbox.Event{xoutbox.NewEvent("", "x", nil), xoutbox.NewEvent("", "y", nil)}) {
		if err != nil {
			t.Fatal(err)
		}
	}
}
