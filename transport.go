package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Transport interface {
	connect() error
	listen(fn process, message interface{}) error
}

// PostgreSQL database transport
type PgDatabase struct {
	Host      string
	Port      uint16
	User      string
	Password  string
	Database  string
	TableName string
}

var pool *pgxpool.Pool

func (database PgDatabase) connect() error {
	var err error
	pool, err = pgxpool.New(context.Background(), fmt.Sprintf("postgres://%s:%s@%s:%d/%s", database.User, database.Password, database.Host, database.Port, database.Database))

	if err != nil {
		return err
	}

	return nil
}

func (database PgDatabase) listen(fn process, message interface{}) error {
	err := database.connect()

	if err != nil {
		return err
	}

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return err
	}

	defer conn.Release()

	log.Printf("Successfully connected to the database!")

	_, err = conn.Exec(context.Background(), fmt.Sprintf("LISTEN %s", database.TableName))
	if err != nil {
		return err
	}

	defer conn.Exec(context.Background(), fmt.Sprintf("UNLISTEN %s", database.TableName))

	for {
		_, err := conn.Conn().WaitForNotification(context.Background())
		if err != nil {
			return err
		}

		var messengerMessage MessengerMessage
		row := conn.QueryRow(context.Background(), fmt.Sprintf("SELECT * FROM %s WHERE delivered_at IS NULL ORDER BY id DESC LIMIT 1", database.TableName))

		if err = row.Scan(&messengerMessage.ID, &messengerMessage.Body, &messengerMessage.Headers, &messengerMessage.QueueName, &messengerMessage.CreatedAt, &messengerMessage.AvailableAt, &messengerMessage.DeliveredAt); err != nil {
			return err
		}

		if messengerMessage.QueueName != "go" {
			continue
		}

		msg, err := formatMessage(messengerMessage.Body, message)
		if err != nil {
			return err
		}

		go fn(msg)
	}
}

// RabbitMQ transport
type RabbitMQ struct {
	Host     string
	Port     *uint8
	User     string
	Password string
	Queue    string
}

var connection *amqp.Connection
var channel *amqp.Channel

func (rabbitmq RabbitMQ) connect() error {
	var err error
	if rabbitmq.Port != nil {
		connection, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/%s", rabbitmq.User, rabbitmq.Password, rabbitmq.Host, rabbitmq.Port, "%2f"))
	} else {
		connection, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", rabbitmq.User, rabbitmq.Password, rabbitmq.Host, "%2f"))
	}

	if err != nil {
		return err
	}

	channel, err = connection.Channel()
	if err != nil {
		return err
	}

	return nil
}

func (rabbitmq RabbitMQ) listen(fn process, message interface{}) error {
	err := rabbitmq.connect()
	if err != nil {
		return err
	}

	defer connection.Close()
	defer channel.Close()

	q, err := channel.QueueDeclare(
		rabbitmq.Queue,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	msgs, err := channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	var forever chan struct{}

	go func() {
		for d := range msgs {
			msg, err := formatMessage(string(d.Body), message)
			if err != nil {
				log.Fatal(err)
			}

			go fn(msg)
		}
	}()

	<-forever

	return nil
}

func formatMessage(message string, msg interface{}) (interface{}, error) {
	if err := json.Unmarshal([]byte(message), &msg); err != nil {
		return msg, err
	}

	return msg, nil
}

func Listen(transport Transport, fn process, message interface{}) error {
	err := transport.listen(fn, message)
	if err != nil {
		return err
	}

	return nil
}
