package main

import (
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

	err <- nil
}
