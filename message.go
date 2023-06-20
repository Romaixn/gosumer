package gosumer

import (
	"time"
)

type MessengerMessage struct {
	ID          int
	Body        string
	Headers     string
	QueueName   string
	CreatedAt   time.Time
	AvailableAt time.Time
	DeliveredAt *time.Time
}

type process func(message any)
