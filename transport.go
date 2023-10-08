package gosumer

import (
	"encoding/json"
)

type Transport interface {
	connect() error
	listen(fn process, message any, sec int) error
}

func formatMessage(message string, msg any) (any, error) {
	if err := json.Unmarshal([]byte(message), &msg); err != nil {
		return msg, err
	}

	return msg, nil
}

func Listen(transport Transport, fn process, message any, sec int) error {
	err := transport.listen(fn, message, sec)
	if err != nil {
		return err
	}

	return nil
}
