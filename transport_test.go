package gosumer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormatMessage(t *testing.T) {
	jsonData := `{"id": 1, "name": "John Doe"}`

	type Payload struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	formatted, err := formatMessage(jsonData, Payload{})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	msg, ok := formatted.(Payload)
	if !ok {
		t.Fatalf("Expected payload type, got %T", formatted)
	}

	assert.Equal(t, 1, msg.ID)
	assert.Equal(t, "John Doe", msg.Name)

	formattedPtr, err := formatMessage(jsonData, &Payload{})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	msgPtr, ok := formattedPtr.(*Payload)
	if !ok {
		t.Fatalf("Expected *Payload type, got %T", formattedPtr)
	}

	assert.Equal(t, 1, msgPtr.ID)
	assert.Equal(t, "John Doe", msgPtr.Name)

	invalidJsonData := `{"id": 1, "name": "John Doe"`

	_, err = formatMessage(invalidJsonData, Payload{})
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
