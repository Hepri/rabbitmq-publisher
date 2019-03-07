package publisher

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/streadway/amqp"
)

const (
	ReconnectTimeout = time.Second * 3
)

type EventConnector struct {
	ctx       context.Context
	closeFunc func()
}

func (ec *EventConnector) Init() {
	ec.ctx, ec.closeFunc = context.WithCancel(context.Background())
}

func (ec *EventConnector) Close() {
	ec.closeFunc()
}

// redial continually connects to the URL, exiting the program when no longer possible
func (ec *EventConnector) connectToExchange(url string, exchanges []string) chan chan session {
	sessions := make(chan chan session)

	go func() {
		defer close(sessions)
		for {
			ctxAlive := ec.initSession(sessions, url, exchanges)
			if !ctxAlive {
				break
			}

			time.Sleep(ReconnectTimeout)
		}
	}()

	return sessions
}

func (ec *EventConnector) initSession(sessions chan chan session, url string, exchanges []string) bool {
	shouldCloseSess := true
	sess := make(chan session)
	defer func() {
		// session should be closed here only if there is no messages in channel
		// because if message exists in channel, end client will receive it and close sess by
		if shouldCloseSess {
			close(sess)
		}
	}()

	logrus.Info("trying to insert sess into sessions")
	select {
	case sessions <- sess:
	case <-ec.ctx.Done():
		logrus.Info("shutting down rabbit session factory")
		return false
	}

	logrus.WithField("url", url).Info("dial rabbitmq")
	conn, err := amqp.Dial(url)
	if err != nil {
		logrus.WithError(err).WithField("url", url).Error("Cannot (re)dial")
		return true
	}

	ch, err := conn.Channel()
	if err != nil {
		logrus.WithError(err).Error("cannot create channel")
		return true
	}

	for _, exchange := range exchanges {
		if err := ch.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil); err != nil {
			logrus.WithError(err).Error("cannot declare fanout exchange")
			return true
		}
	}

	select {
	case sess <- session{conn, ch}:
	case <-ec.ctx.Done():
		logrus.Info("shutting down new session")
		return false
	}

	shouldCloseSess = false
	return true
}
