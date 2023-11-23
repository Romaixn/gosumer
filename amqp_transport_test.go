package gosumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestAmqpConnect(t *testing.T) {
	transport := RabbitMQ{
		Host:     "localhost",
		Port:     nil,
		User:     "guest",
		Password: "guest",
		Queue:    "queue_name",
	}

	err := transport.connect()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestAmqpListen(t *testing.T) {
	transport := RabbitMQ{
		Host:     "localhost",
		Port:     nil,
		User:     "guest",
		Password: "guest",
		Queue:    "queue_name",
	}

	go func() {
		err := transport.listen(processMessage, Message{}, 0)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	}()

	connection, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", transport.User, transport.Password, transport.Host, "%2f"))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	defer channel.Close()

	q, err := channel.QueueDeclare(
		transport.Queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := `{"id": 1, "name": "John Doe"}`
	err = channel.PublishWithContext(ctx,
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// TODO: maybe found a better way to wait for the message to be processed
	time.Sleep(1 * time.Second)
	assert.True(t, processMessageCalled, "Expected processMessage to be called")
	processMessageCalled = false
}
