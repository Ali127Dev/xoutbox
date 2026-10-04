package rabbitmq_test

import (
	"context"
	"os"
	"testing"

	"github.com/Ali127Dev/xoutbox"
	"github.com/Ali127Dev/xoutbox/rabbitmq"
)

// XOUTBOX_RABBITMQ_URL=amqp://guest:guest@localhost:5672/
func TestPublisher(t *testing.T) {
	url := os.Getenv("XOUTBOX_RABBITMQ_URL")
	if url == "" {
		t.Skip("XOUTBOX_RABBITMQ_URL not set")
	}
	p, err := rabbitmq.New(rabbitmq.Config{URL: url, DefaultRoutingKey: "xoutbox-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()
	if err := p.Publish(ctx, xoutbox.NewEvent("", "x", []byte("a"))); err != nil {
		t.Fatal(err)
	}
	for _, err := range p.PublishBatch(ctx, []xoutbox.Event{xoutbox.NewEvent("", "x", nil), xoutbox.NewEvent("", "y", nil)}) {
		if err != nil {
			t.Fatal(err)
		}
	}
}
