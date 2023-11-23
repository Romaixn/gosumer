package gosumer

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	Host     string
	Port     *uint8
	User     string
	Password string
	Queue    string
}

var connection *amqp.Connection
var channel *amqp.Channel
var _ Transport = (*RabbitMQ)(nil)

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

func (rabbitmq RabbitMQ) listen(fn process, message any, _ int) error {
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
