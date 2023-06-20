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
	database := gosumer.RabbitMQ{
		Host:     "localhost",
		Port:     nil,
		User:     "admin",
		Password: "admin",
		Queue:    "golang",
	}

	err := gosumer.Listen(database, processMessage, Message{})

	if err != nil {
		log.Fatal(err)
	}
}

func processMessage(message any) {
	log.Printf("Message received: %v", message)
}
