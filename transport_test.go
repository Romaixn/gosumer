package gosumer

import (
	"testing"
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

func (transport TestTransport) listen(fn process, message any) error {
	return nil
}

func processMessage(message any, err chan error) {
	err <- nil
}

type Message struct {
	ID int `json:"id"`
}

func TestListen(t *testing.T) {
	transport := TestTransport{}

	err := Listen(transport, processMessage, Message{})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}
