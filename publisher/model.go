package publisher

import (
	"time"

	"github.com/streadway/amqp"
)

// session composes an amqp.Connection with an amqp.Channel
type session struct {
	*amqp.Connection
	*amqp.Channel
}

// Close tears the connection down, taking the channel with it.
func (s session) Close() error {
	if s.Connection == nil {
		return nil
	}
	return s.Connection.Close()
}

type Message struct {
	Exchange    string
	Type        string
	Body        []byte
	Sub         session
	DeliveryTag uint64
}

func (this *Message) Success() {
	this.Sub.Ack(this.DeliveryTag, false)
}

func (this *Message) Fail() {
	// default redelivery time
	time.Sleep(time.Second * 3)
	this.Sub.Nack(this.DeliveryTag, false, true)
}
