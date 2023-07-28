package gosumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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

func (database PgDatabase) listenEvery(seconds int, fn process, message any) {
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
