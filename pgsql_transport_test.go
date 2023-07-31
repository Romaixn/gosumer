package gosumer

import (
	"context"
	"fmt"
	"testing"

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

func dropAndCreateTable(pool *pgxpool.Pool, tableName string) {
	pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	pool.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (id BIGSERIAL NOT NULL, body TEXT NOT NULL, headers TEXT NOT NULL, queue_name VARCHAR(190) NOT NULL, created_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL, available_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL, delivered_at TIMESTAMP(0) WITHOUT TIME ZONE DEFAULT NULL, PRIMARY KEY(id))", tableName))
}

func TestPgDelete(t *testing.T) {
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

	dropAndCreateTable(pool, database.TableName)

	_, err = pool.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (body, headers, queue_name, created_at, available_at, delivered_at) VALUES ('{\"id\": 1}', '{}', 'go', NOW(), NOW(), NULL)", database.TableName))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	err = database.connect()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	err = database.delete(1)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestPgProcessMessage(t *testing.T) {
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

	dropAndCreateTable(pool, database.TableName)

	_, err = pool.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (body, headers, queue_name, created_at, available_at, delivered_at) VALUES ('{\"id\": 1}', '{}', 'go', NOW(), NOW(), NULL)", database.TableName))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	err = database.connect()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	err = database.processMessage(processMessage, Message{})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	assert.True(t, processMessageCalled)
}
