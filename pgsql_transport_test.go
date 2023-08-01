package gosumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestPgConnect(t *testing.T) {
	database := PgDatabase{
		Host:      "localhost",
		Port:      5432,
		User:      "postgres",
		Password:  "postgres",
		Database:  "postgres",
		TableName: "table_name",
	}

	err := database.connect()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func setupDatabase(t *testing.T) (*pgxpool.Pool, PgDatabase) {
	database := PgDatabase{
		Host:      "localhost",
		Port:      5432,
		User:      "postgres",
		Password:  "postgres",
		Database:  "postgres",
		TableName: "table_name",
	}

	pool, err := pgxpool.New(context.Background(), fmt.Sprintf("postgres://%s:%s@%s:%d/%s", database.User, database.Password, database.Host, database.Port, database.Database))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", database.TableName))
	pool.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (id BIGSERIAL NOT NULL, body TEXT NOT NULL, headers TEXT NOT NULL, queue_name VARCHAR(190) NOT NULL, created_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL, available_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL, delivered_at TIMESTAMP(0) WITHOUT TIME ZONE DEFAULT NULL, PRIMARY KEY(id))", database.TableName))

	_, err = pool.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (body, headers, queue_name, created_at, available_at, delivered_at) VALUES ('{\"id\": 1}', '{}', 'go', NOW(), NOW(), NULL)", database.TableName))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	err = database.connect()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	return pool, database
}

func TestPgDelete(t *testing.T) {
	pool, database := setupDatabase(t)
	defer pool.Close()

	err := database.delete(1)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestPgProcessMessage(t *testing.T) {
	pool, database := setupDatabase(t)
	defer pool.Close()

	err := database.processMessage(processMessage, Message{})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	assert.True(t, processMessageCalled)
	processMessageCalled = false
}

func TestPgListen(t *testing.T) {
	pool, database := setupDatabase(t)
	defer pool.Close()

	go func() {
		err := database.listen(processMessage, Message{})
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	}()

	_, err := pool.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (body, headers, queue_name, created_at, available_at, delivered_at) VALUES ('{\"id\": 2}', '{}', 'go', NOW(), NOW(), NULL)", database.TableName))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	row := pool.QueryRow(context.Background(), fmt.Sprintf("SELECT delivered_at FROM %s WHERE id = 2", database.TableName))
	var deliveredAt time.Time
	err = row.Scan(&deliveredAt)
	if err == nil {
		t.Errorf("Message was not processed: %v", err)
	}

	// TODO: maybe found a better way to wait for the message to be processed
	time.Sleep(6 * time.Second)
	assert.True(t, processMessageCalled)
	processMessageCalled = false
}

func TestPgListenEvery(t *testing.T) {
	pool, database := setupDatabase(t)
	defer pool.Close()

	go func() {
		database.listenEvery(1, processMessage, Message{})
	}()

	row := pool.QueryRow(context.Background(), fmt.Sprintf("SELECT delivered_at FROM %s WHERE id = 1", database.TableName))
	var deliveredAt time.Time
	err := row.Scan(&deliveredAt)
	if err == nil {
		t.Errorf("message was not processed: %v", err)
	}

	// TODO: maybe found a better way to wait for the message to be processed
	time.Sleep(2 * time.Second)
	assert.True(t, processMessageCalled)
	processMessageCalled = false
}
