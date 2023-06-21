# Gosumer

⚡ Improve your application's performance by consuming your Symfony Messenger messages with Go.

## ✨ Features

- Consume your messages directly with Go code
- PostgreSQL support
- AMQP support

## Installation
Install gosumer with Go

```bash
go get github.com/romaixn/gosumer
```

## ⚙️ Configuration
### PostgreSQL
Add this to your `config/packages/messenger.yaml`:
```yaml
framework:
    messenger:
        transports:
            go: # Add this new transport
                dsn: '%env(MESSENGER_TRANSPORT_DSN)%'
                serializer: 'messenger.transport.symfony_serializer' # Required, https://symfony.com/doc/current/messenger.html#serializing-messages
                options:
                    use_notify: true
                    check_delayed_interval: 60000
                    queue_name: go # Required, used to only get right messages in go side
                retry_strategy:
                    max_retries: 3
                    multiplier: 2
```

Don't forget to specify in the `routing` part the message to process in Go


### RabbitMQ
Create a env variable to create a custom queue (in this example `go` is the name of the queue):
```
RABBITMQ_GO_TRANSPORT_DSN=amqp://guest:guest@localhost:5672/%2f/go
```

And use it in `config/packages/messenger.yaml`:

```yaml
framework:
    messenger:
        transports:
            go:
                dsn: '%env(RABBITMQ_GO_TRANSPORT_DSN)%'
                serializer: 'messenger.transport.symfony_serializer'
                retry_strategy:
                    max_retries: 3
                    multiplier: 2
```

## Usage
### Configure the transport
For PostgreSQL:
```go
database := gosumer.PgDatabase{
    Host:      "localhost",
    Port:      5432,
    User:      "app",
    Password:  "!ChangeMe!",
    Database:  "app",
    TableName: "messenger_messages",
}
```

For RabbitMQ:
```go
database := gosumer.RabbitMQ{
    Host:     "localhost",
    Port:     nil,
    User:     "guest",
    Password: "guest",
    Queue:    "go",
}
```

### Listen for messages
Call the Listen
```go
// Define your own structure according to your message
type Message struct {
	ID     int `json:"id"`
	Number int `json:"number"`
}

err := gosumer.Listen(database, process, Message{})

if err != nil {
    log.Fatal(err)
}
```

With the function to process your messages:
```go
func process(message any, err chan error) {
    log.Printf("Message received: %v", message)

    // No error
    err <- nil

    // if there is an error, used to not delete message if an error occured
    // err <- errors.New("Error occured !")
}
```
