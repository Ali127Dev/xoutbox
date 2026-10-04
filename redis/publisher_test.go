package redis_test

import (
	"context"
	"os"
	"testing"

	"github.com/Ali127Dev/xoutbox"
	xredis "github.com/Ali127Dev/xoutbox/redis"
	goredis "github.com/redis/go-redis/v9"
)

// XOUTBOX_REDIS_ADDR=localhost:6379
func TestPublisher(t *testing.T) {
	addr := os.Getenv("XOUTBOX_REDIS_ADDR")
	if addr == "" {
		t.Skip("XOUTBOX_REDIS_ADDR not set")
	}
	c := goredis.NewClient(&goredis.Options{Addr: addr})
	defer c.Close()
	ctx := context.Background()
	c.Del(ctx, "xoutbox-test")
	p := xredis.New(c, xredis.Config{DefaultStream: "xoutbox-test", MaxLen: 1000})
	if err := p.Publish(ctx, xoutbox.NewEvent("", "x", []byte("a")).WithHeader("h", "v")); err != nil {
		t.Fatal(err)
	}
	for _, err := range p.PublishBatch(ctx, []xoutbox.Event{xoutbox.NewEvent("", "x", nil), xoutbox.NewEvent("", "y", nil)}) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := c.XLen(ctx, "xoutbox-test").Val(); n != 3 {
		t.Fatalf("stream length %d", n)
	}
}
