package nats_test

import (
	"context"
	"os"
	"testing"

	"github.com/Ali127Dev/xoutbox"
	xnats "github.com/Ali127Dev/xoutbox/nats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// XOUTBOX_NATS_URL=nats://localhost:4222 (server started with -js)
func TestPublisherDedup(t *testing.T) {
	url := os.Getenv("XOUTBOX_NATS_URL")
	if url == "" {
		t.Skip("XOUTBOX_NATS_URL not set")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx := context.Background()
	_ = js.DeleteStream(ctx, "XOUTBOX")
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "XOUTBOX", Subjects: []string{"events.>"}})
	if err != nil {
		t.Fatal(err)
	}
	p := xnats.New(js, xnats.WithSubjectPrefix("events."))
	e := xoutbox.NewEvent("orders", "x", []byte("a"))
	if err := p.Publish(ctx, e); err != nil {
		t.Fatal(err)
	}
	for _, err := range p.PublishBatch(ctx, []xoutbox.Event{e, xoutbox.NewEvent("orders", "y", nil)}) {
		if err != nil {
			t.Fatal(err)
		}
	}
	info, _ := s.Info(ctx)
	if info.State.Msgs != 2 {
		t.Fatalf("want 2 messages after dedup, got %d", info.State.Msgs)
	}
}
