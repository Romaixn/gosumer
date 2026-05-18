package gosumer

import (
	"encoding/json"
	"reflect"
)

type Transport interface {
	connect() error
	listen(fn process, message any, sec int) error
}

func formatMessage(message string, msg any) (any, error) {
	msgType := reflect.TypeOf(msg)
	if msgType == nil {
		var formatted any
		if err := json.Unmarshal([]byte(message), &formatted); err != nil {
			return nil, err
		}

		return formatted, nil
	}

	target := reflect.New(msgType)
	if msgType.Kind() == reflect.Ptr {
		target = reflect.New(msgType.Elem())
	}

	if err := json.Unmarshal([]byte(message), target.Interface()); err != nil {
		return nil, err
	}

	if msgType.Kind() == reflect.Ptr {
		return target.Interface(), nil
	}

	return target.Elem().Interface(), nil
}

func Listen(transport Transport, fn process, message any, sec int) error {
	err := transport.listen(fn, message, sec)
	if err != nil {
		return err
	}

	return nil
}

func executeProcess(fn process, message any) error {
	errChan := make(chan error, 1)
	go fn(message, errChan)

	return <-errChan
}
