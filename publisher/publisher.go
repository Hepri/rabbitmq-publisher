package publisher

import (
	"time"

	"database/sql"
	"fmt"

	"github.com/Hepri/rabbitmq_publisher/model"
	"github.com/sirupsen/logrus"

	"github.com/lib/pq"
	"github.com/streadway/amqp"
)

const (
	NotifyTimeout = 20 * time.Second
)

const (
	getEventSql = `
		select id,
		       status,
		       exchange,
		       type,
		       data
		  from amqp_events
		 where status = $1
		 order by id
		 limit 1
		 for update`

	completeEventSql = `
		update amqp_events
		   set status = $1,
		       send_date = $2
		 where id = $3`
)

type EventPublisher struct {
	EventConnector
	db       *sql.DB
	listener *pq.Listener

	url       string
	exchanges []string
	queue     string
	messages  chan *Message
}

func (ev *EventPublisher) Start() {
	ev.messages = make(chan *Message)
	go func() {
		ev.startPublishing(ev.connectToExchange(ev.url, ev.exchanges))
		defer close(ev.messages)
	}()
}

func (ev *EventPublisher) Stop() {
	ev.Close()
}

// subscribe consumes deliveries from an exclusive queue from a fanout exchange and sends to the application specific messages chan.
func (ev *EventPublisher) startPublishing(sessions chan chan session) {
	for session := range sessions {
		pub, alive := <-session

		if !alive {
			continue
		}

		for {
			err := ev.processMessage(pub)
			if err != nil {
				break
			}
		}

		close(session)
	}
}

func (ev *EventPublisher) runInsideTransaction(fn func(tx *sql.Tx) error) error {
	tx, err := ev.db.Begin()
	if err != nil {
		logrus.WithError(err).Error("cannot start db transaction")
		return err
	}

	// Rollback the transaction on panics in the action. Don't swallow the
	// panic, though, let it propagate.
	defer func(tx *sql.Tx) {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
	}(tx)

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (ev *EventPublisher) processMessage(sess session) error {
	var (
		evID       int
		evStatus   int
		evExchange string
		evType     string
		evData     string
	)

	return ev.runInsideTransaction(func(tx *sql.Tx) error {
		err := tx.QueryRow(getEventSql, model.EventStatusPending).Scan(&evID, &evStatus, &evExchange, &evType, &evData)
		if err != nil {
			// event absence is not an error wait for next process cycle
			if err == sql.ErrNoRows {
				err = nil

				// by default we should catch notify event and start next processing step
				// if event is not emitted during "NotifyTimeout" then go to next processing step immediately
				select {
				case <-time.After(NotifyTimeout):
					break
				case <-ev.listener.Notify:
					break
				}
			} else {
				logrus.WithError(err).Error("error during query row")
			}

			return nil
		}

		// publish event to amqp
		err = ev.publishEvent(sess, &model.AMQPEvent{
			ID:       evID,
			Status:   evStatus,
			Exchange: evExchange,
			Type:     evType,
			Data:     evData,
		})

		if err != nil {
			logrus.WithError(err).Error("error during publish event to rabbitmq")
			return err
		}

		_, err = tx.Exec(completeEventSql, model.EventStatusComplete, time.Now().In(time.UTC), evID)
		if err != nil {
			logrus.WithError(err).Error("error during set amqp_event complete")
			return err
		}

		return nil
	})
}

func (ev *EventPublisher) publishEvent(sess session, event *model.AMQPEvent) error {
	// start transaction
	err := sess.Tx()
	if err != nil {
		logrus.WithError(err).Error("cannot start Tx")
		return err
	}

	body := fmt.Sprintf(`{"id":"%d", "data": %s}`, event.ID, event.Data)

	// try to publish into channel
	logrus.WithField("id", event.ID).Info("publish event")

	err = sess.Publish(event.Exchange, "", false, false, amqp.Publishing{
		// use persistent delivery mode (mode = 2) for maximum consistency
		DeliveryMode: 2,
		Type:         event.Type,
		Body:         []byte(body),
	})
	if err != nil {
		logrus.WithError(err).Error("cannot publish")
		sess.TxRollback()
		return err
	}

	// success make commit to rmq and db
	err = sess.TxCommit()
	if err != nil {
		logrus.WithError(err).Error("cannot commit Tx")
		return err
	}

	return nil
}

func NewEventPublisher(url string, exchanges []string, db *sql.DB, listener *pq.Listener) *EventPublisher {
	publisher := &EventPublisher{
		db:        db,
		listener:  listener,
		url:       url,
		exchanges: exchanges,
	}
	publisher.Init()
	return publisher
}
