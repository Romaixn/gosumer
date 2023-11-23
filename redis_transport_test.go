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
		t.Errorf("Expected no error, got %v", err)
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

	go func() {
		err := transport.listen(processMessage, Message{}, 0)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	}()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	time.Sleep(1 * time.Second)

	ctx := context.Background()
	body := `{"id": 1, "name": "John Doe"}`
	res := rdb.Publish(ctx, "channel_1", body)

	if res.Err() != nil {
		t.Errorf("Expected no error, got %v", res.Err())
	}
	time.Sleep(1 * time.Second)

	assert.True(t, processMessageCalled, "Expected processMessage to be called")
	processMessageCalled = false
}
