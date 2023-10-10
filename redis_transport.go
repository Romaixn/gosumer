package gosumer

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	Host     string
	Port     int
	User     string
	Password string
	DB       uint8
	Channel  string
}

var rdb *redis.Client

func (red Redis) connect() error {
	var err error
	rdb = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", red.Host, red.Port),
		Username: red.User,
		Password: red.Password,
		DB:       int(red.DB),
	})
	ctx := context.Background()
	err = rdb.Ping(ctx).Err()

	if err != nil {
		return err
	}

	return nil
}

func (red Redis) listen(fn process, message any) error {
	err := red.connect()
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}
	ctx := context.Background()
	sub := rdb.Subscribe(ctx, red.Channel)
	defer sub.Close()
	for {
		msg, err := sub.ReceiveMessage(ctx)
		if err != nil {
			panic(err)
		}
		var e chan error
		go fn(msg, e)
	}
}
