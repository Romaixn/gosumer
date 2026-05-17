package gosumer

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestRedisConnect(t *testing.T) {
	transport := Redis{
		Host:    "localhost",
		Port:    6379,
		Channel: "channel_name",
	}

	err := transport.connect()
	if err != nil {
		t.Skipf("Redis is not available: %v", err)
	}
}

func TestRedisListen(t *testing.T) {
	transport := Redis{
		Host:     "localhost",
		Port:     6379,
		User:     "",
		Password: "",
		Channel:  "channel_1",
		DB:       0,
	}

	if err := transport.connect(); err != nil {
		t.Skipf("Redis is not available: %v", err)
		return
	}

	go func() {
		_ = transport.listen(processMessage, Message{}, 0)
	}()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	time.Sleep(1 * time.Second)

	ctx := context.Background()
	body := `{"id": 1, "name": "John Doe"}`
	res := rdb.Publish(ctx, "channel_1", body)

	if res.Err() != nil {
		t.Skipf("Redis is not available: %v", res.Err())
	}
	time.Sleep(1 * time.Second)

	assert.True(t, processMessageCalled, "Expected processMessage to be called")
	processMessageCalled = false
}
