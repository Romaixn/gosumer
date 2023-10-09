package gosumer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormatMessage(t *testing.T) {
	var msg any

	jsonData := `{"id": 1, "name": "John Doe"}`

	_, err := formatMessage(jsonData, &msg)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	invalidJsonData := `{"id": 1, "name": "John Doe"`

	_, err = formatMessage(invalidJsonData, &msg)
	if err == nil {
		t.Errorf("Expected an error for invalid JSON data, but got none")
	}
}

type TestTransport struct{}

func (transport TestTransport) connect() error {
	return nil
}

func (transport TestTransport) listen(fn process, message any, sec int) error {
	go fn(message, make(chan error))

	return nil
}

var processMessageCalled = false

func processMessage(message any, err chan error) {
	processMessageCalled = true
	err <- nil
}

type Message struct {
	ID int `json:"id"`
}

func TestListen(t *testing.T) {
	transport := TestTransport{}

	err := Listen(transport, processMessage, Message{}, 5)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// TODO: maybe found a better way to wait for the message to be processed
	time.Sleep(1 * time.Second)
	assert.True(t, processMessageCalled)
	processMessageCalled = false
}
