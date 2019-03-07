package model

import "time"

const (
	EventStatusPending = 1
	EventStatusComplete = 2
)

type AMQPEvent struct {
	ID         int
	CreateDate time.Time
	Status     int
	Exchange   string
	Type       string
	Data       string
}
