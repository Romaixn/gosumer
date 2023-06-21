package main

import (
	"encoding/json"
	"log"

	"github.com/romaixn/gosumer"
)

type Message struct {
	ID     int `json:"id"`
	Number int `json:"number"`
}

func main() {
	database := gosumer.PgDatabase{
		Host:      "localhost",
		Port:      5432,
		User:      "app",
		Password:  "!ChangeMe!",
		Database:  "app",
		TableName: "messenger_messages",
	}

	err := gosumer.Listen(database, processMessage, Message{})

	if err != nil {
		log.Fatal(err)
	}
}

func processMessage(message any, err chan error) {
	log.Printf("Message received: %v", message)

	jsonData, e := json.Marshal(message)
	if e != nil {
		log.Printf("Error marshalling message: %v", e)
		err <- e

		return
	}

	var msg Message
	e = json.Unmarshal(jsonData, &msg)
	if e != nil {
		log.Printf("Error unmarshalling message: %v", e)
		err <- e

		return
	}

	log.Printf("Sum of %d and %d is %d", msg.ID, msg.Number, msg.ID+msg.Number)

	err <- nil

	return
}
