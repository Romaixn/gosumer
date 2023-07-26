package gosumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Transport interface {
	connect() error
	listen(fn process, message any) error
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

func (database PgDatabase) listen(fn process, message any) error {
	err := database.connect()

	if err != nil {
		return err
	}

	database.listenEvery(5, fn, message)

	log.Printf("Successfully connected to the database!")

	_, err = pool.Exec(context.Background(), fmt.Sprintf("LISTEN %s", database.TableName))
	if err != nil {
		return err
	}

	defer pool.Exec(context.Background(), fmt.Sprintf("UNLISTEN %s", database.TableName))

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return err
	}

	defer conn.Release()

	for {
		_, err := conn.Conn().WaitForNotification(context.Background())
		if err != nil {
			return err
		}

		err = database.processMessage(fn, message)
		if err != nil {
			continue
		}
	}
}

func (database PgDatabase) listenEvery(seconds int, fn process, message any) error {
	ticker := time.NewTicker(time.Duration(seconds) * time.Second)

	go func() error {
		for {
			select {
			case <-ticker.C:
				err := database.processMessage(fn, message)
				if err != nil {
					continue
				}
			}
		}
	}()

	return nil
}

func (database PgDatabase) delete(id int) error {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return err
	}

	defer conn.Release()

	_, err = conn.Exec(context.Background(), fmt.Sprintf("DELETE FROM %s WHERE id = %d", database.TableName, id))
	if err != nil {
		return err
	}

	return nil
}

func (database PgDatabase) processMessage(fn process, message any) error {
	var messengerMessage MessengerMessage
	row := pool.QueryRow(context.Background(), fmt.Sprintf("SELECT * FROM %s WHERE delivered_at IS NULL AND queue_name = 'go' ORDER BY id DESC LIMIT 1", database.TableName))

	if err := row.Scan(&messengerMessage.ID, &messengerMessage.Body, &messengerMessage.Headers, &messengerMessage.QueueName, &messengerMessage.CreatedAt, &messengerMessage.AvailableAt, &messengerMessage.DeliveredAt); err != nil {
		return err
	}

	msg, err := formatMessage(messengerMessage.Body, message)
	if err != nil {
		return err
	}

	e := make(chan error)
	go fn(msg, e)

	processErr := <-e
	if processErr != nil {
		return processErr
	}

	go database.delete(messengerMessage.ID)

	return nil
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

func (rabbitmq RabbitMQ) listen(fn process, message any) error {
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

	log.Printf("Successfully connected to the queue!")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			msg, err := formatMessage(string(d.Body), message)
			if err != nil {
				log.Fatal(err)
			}

			var e chan error
			go fn(msg, e)
		}
	}()

	<-forever

	return nil
}

func formatMessage(message string, msg any) (any, error) {
	if err := json.Unmarshal([]byte(message), &msg); err != nil {
		return msg, err
	}

	return msg, nil
}

func Listen(transport Transport, fn process, message any) error {
	err := transport.listen(fn, message)
	if err != nil {
		return err
	}

	return nil
}
